from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_date, date_sub, get_json_object, from_json, 
    explode, explode_outer, posexplode, row_number, max as spark_max, 
    when, coalesce, to_timestamp, concat, trim, lower, upper, expr, array
)
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, MapType, IntegerType
from pyspark.sql.window import Window

# Note: SparkSession is assumed to be available as 'spark'

# ==============================================================================
# Step 1: Preparation & Variable Definition
# ==============================================================================

# Define Start and End Dates equivalent to SQL: CURRENT_DATE-1
# We use date_sub(current_date(), 1)
start_date_val = date_sub(current_date(), 1)
end_date_val = date_sub(current_date(), 1)

# Helper function to mimic TRY_CAST or implicit casting safely
def safe_cast(col_obj, data_type):
    return when(col_obj == "", lit(None)).otherwise(col_obj).cast(data_type)

# ==============================================================================
# Step 2: Create First Temporary Table (Equivalent to ...02389c9ef7f103b2)
# ==============================================================================
# SQL Logic:
# 1. CTE PLACE_ORDER: Filter 'place_order_source' from JSON.
# 2. CTE DP_PAYMENT_API: Join source with PLACE_ORDER.
# 3. Main Logic: Explode PAYMENT_GROUP, apply window functions, filter.

# 2.1 Load Source Tables
# Removing database/schema parts for source tables as per instructions, keeping table name.
df_place_order_source = spark.read.format("delta").table("883058003aebca26")
df_dp_payment_source = spark.read.format("delta").table("de68711fcef34220")

# 2.2 Transform PLACE_ORDER
# Extract 'place_order_source' from 'REQUEST_PARAMS' JSON string
df_po = df_place_order_source.alias("po").filter(
    (col("DT").between(start_date_val, end_date_val))
).withColumn(
    "place_order_source", 
    get_json_object(col("REQUEST_PARAMS"), "$.place_order_source").cast("string")
).filter(
    col("place_order_source") == "turbocheckout"
).groupBy(
    col("DT"), col("SID"), col("place_order_source")
).count().select("DT", "SID") # Group by implies distinct here for join purposes

# 2.3 Transform DP_PAYMENT_API (Inner Query A)
df_dp_raw = df_dp_payment_source.alias("raw").filter(
    col("DT").between(start_date_val, end_date_val)
).select(
    col("DT"),
    col("TIME_STAMP"),
    col("TENANT"),
    col("customer_id"),
    col("BASIC_DATA"),
    get_json_object(col("BASIC_DATA"), "$.sid").alias("sid"),
    col("RESTAURANT_ID"),
    col("PAYMENT_GROUP"),
    col("ADDITIONAL_DETAILS")
)

# Join with PLACE_ORDER
df_dp_joined = df_dp_raw.join(
    df_po.alias("b"),
    (df_dp_raw.SID == col("b.SID")) & (df_dp_raw.DT == col("b.DT")),
    "left"
).select(
    df_dp_raw["*"],
    col("b.SID").alias("dp_place_order_sid")
)

# 2.4 Explode PAYMENT_GROUP and Calculate Window Functions
# Note: PAYMENT_GROUP seems to be a JSON array string. We need to parse it to explode.
# Schema assumption based on usage: Array of objects containing "group_name"
json_schema_payment_group = ArrayType(MapType(StringType(), StringType()))

df_exploded = df_dp_joined.withColumn(
    "B_VALUE", explode_outer(from_json(col("PAYMENT_GROUP"), json_schema_payment_group))
)

# Extract fields for calculation
df_calc_prep = df_exploded.select(
    col("DT"),
    to_timestamp(col("TIME_STAMP")).alias("ROUNDED_TIME"),
    get_json_object(col("BASIC_DATA"), "$.request_id").alias("REQUEST_ID"),
    col("sid"),
    get_json_object(col("ADDITIONAL_DETAILS"), "$.page_visited").alias("PAGE"),
    col("PAYMENT_GROUP"),
    col("dp_place_order_sid"),
    col("TENANT"),
    # TC_FLAG: case when group_name = 'single_click' then 1 else 0
    when(col("B_VALUE").getItem("group_name") == "single_click", 1).otherwise(0).alias("TC_FLAG"),
    # PREF_FLAG: case when group_name = 'preferred' then 1 else 0
    when(col("B_VALUE").getItem("group_name") == "preferred", 1).otherwise(0).alias("PREF_FLAG"),
    # P_FLAG: case when page = 'payment' then 1 else 0
    when(get_json_object(col("ADDITIONAL_DETAILS"), "$.page_visited") == "payment", 1).otherwise(0).alias("P_FLAG")
).filter(
    (col("DT").between(start_date_val, end_date_val)) &
    (col("TENANT").isin('SW-FOOD', 'SW-FOOD-PWA'))
)

# Window Definitions
w_req = Window.partitionBy("REQUEST_ID")
w_sid = Window.partitionBy("SID")

df_windowed = df_calc_prep.withColumn("FINALFLAG", spark_max("TC_FLAG").over(w_req)) \
    .withColumn("S_FINALFLAG", spark_max("TC_FLAG").over(w_sid)) \
    .withColumn("P_SESSION_FLAG", spark_max("P_FLAG").over(w_sid)) \
    .withColumn("PREFERRED_FLAG", spark_max("PREF_FLAG").over(w_sid))

# 2.5 Final Logic for Temp Table 1 (Calculated Columns + Filtering)
df_temp1_logic = df_windowed.withColumn(
    "dp_sid", col("dp_place_order_sid")
).withColumn(
    "TURBO_FLAG",
    when((col("PAGE") == 'payment') & (col("FINALFLAG") == 0) & (col("S_FINALFLAG") == 0), 'F')
    .when((col("PAGE") == 'checkout') & (col("FINALFLAG") == 1) & (col("dp_place_order_sid").isNotNull()), 'T')
    .when((col("PAGE") == 'payment') & (col("FINALFLAG") == 0) & (col("S_FINALFLAG") == 1), 'T')
).withColumn(
    "TURBO_TYPE",
    when((col("PAGE") == 'payment') & (col("FINALFLAG") == 0) & (col("S_FINALFLAG") == 0), 'NA')
    .when((col("PAGE") == 'checkout') & (col("FINALFLAG") == 1) & (col("dp_place_order_sid").isNotNull()), 'TURBO-SLIDE')
    .when((col("PAGE") == 'payment') & (col("FINALFLAG") == 0) & (col("S_FINALFLAG") == 1), 'TURBO-CHANGE')
)

# Row Number for Deduplication
w_rk = Window.partitionBy("SID").orderBy(col("ROUNDED_TIME").desc())
df_temp1_rk = df_temp1_logic.withColumn("rk", row_number().over(w_rk))

# Condition Met Calculation
cond_expr = (
    (~((col("PAGE") == 'checkout') & col("dp_sid").isNull() & (col("P_SESSION_FLAG") == 0))) &
    (~((col("PAGE") == 'checkout') & col("TURBO_TYPE").isNull())) &
    (~((col("PAGE") == 'payment') & col("TURBO_TYPE").isNull()))
)

df_temp1_final = df_temp1_rk.withColumn(
    "condition_met", when(cond_expr, 1).otherwise(0)
).filter(
    (col("condition_met") == 1) & (col("rk") == 1)
).select(
    "DT", "REQUEST_ID", "SID", "FINALFLAG", "S_FINALFLAG", "PAGE", "dp_sid", 
    "P_FLAG", "P_SESSION_FLAG", "PREF_FLAG", "PREFERRED_FLAG", 
    "TURBO_FLAG", "TURBO_TYPE", "PAYMENT_GROUP"
)

# Write to intermediate Delta table to break lineage and mimic temp table
temp_table_1_name = "temp_02389c9ef7f103b2"
df_temp1_final.write.format("delta").mode("overwrite").saveAsTable(temp_table_1_name)

# ==============================================================================
# Step 3: Create Second Temporary Table (Equivalent to ...5fe47e35e26cf162)
# ==============================================================================
# SQL Logic:
# Read Temp Table 1.
# Nested Flatten: Explode PAYMENT_GROUP -> Explode payment_methods (with index).
# Join IAM table.
# Aggregate to pivot payment details.

df_temp1_read = spark.read.table(temp_table_1_name)
df_iam = spark.read.format("delta").table("30eeae34db51e44e")

# Parse JSON Structures for Nested Explode
# We need to parse PAYMENT_GROUP to an array of structs to access nested 'payment_methods'
# Structure: [{group_name: ..., payment_methods: [{payment_code:..., enabled:..., name:...}]}]
schema_inner_payment = ArrayType(StructType([
    StructField("payment_code", StringType()),
    StructField("enabled", StringType()),
    StructField("name", StringType())
]))
schema_outer_group = ArrayType(StructType([
    StructField("group_name", StringType()),
    StructField("payment_methods", StringType()) # Keeping as string first to parse inner later or struct if json is clean
]))

# 1. Explode PAYMENT_GROUP with Index (F)
df_f = df_temp1_read.select(
    col("DT"), col("SID"), 
    posexplode(from_json(col("PAYMENT_GROUP"), schema_outer_group)).alias("GROUP_INDEX", "F_VAL")
)

# 2. Explode payment_methods with Index (F1)
# Note: F_VAL.payment_methods is likely a string or struct depending on JSON. SQL suggests accessing properties.
df_f1 = df_f.select(
    col("DT"), col("SID"), col("GROUP_INDEX"), col("F_VAL.group_name").alias("GROUP_NAME"),
    posexplode(from_json(col("F_VAL.payment_methods"), schema_inner_payment)).alias("PAYMENT_INDEX", "F1_VAL")
)

# 3. Extract attributes
df_p = df_f1.select(
    col("DT"), col("SID"), col("GROUP_NAME"), col("GROUP_INDEX"), col("PAYMENT_INDEX"),
    col("F1_VAL.payment_code").alias("PAYMENT_NAME"),
    col("F1_VAL.enabled").alias("ENABLED"),
    lower(col("F1_VAL.name")).alias("name"),
    when(col("GROUP_NAME") == "preferred", 1).otherwise(0).alias("PREFERRED_FLAG")
)

# Join IAM
df_p_joined = df_p.alias("P").join(
    df_iam.alias("IAM"),
    col("IAM.PACKAGE_NAME") == col("P.name"),
    "left"
)

# Calculate INTENT_APP
df_p_aug = df_p_joined.withColumn(
    "INTENT_APP",
    when((col("P.PAYMENT_NAME") == "UPIIntent") & col("IAM.PACKAGE_NAME").isNotNull(), col("IAM.INTENT_APP"))
    .when(col("P.PAYMENT_NAME") != "UPIIntent", "INTENT N.A.")
    .otherwise("OTHER INTENT APP")
)

# Aggregation (Pivoting)
df_temp2_final = df_p_aug.groupBy("DT", "SID").agg(
    spark_max(when((col("GROUP_NAME") == "preferred") & (col("PAYMENT_INDEX") == 0), col("PAYMENT_NAME"))).alias("PPM1"),
    spark_max(when((col("GROUP_NAME") == "preferred") & (col("PAYMENT_INDEX") == 1), concat(col("PAYMENT_NAME"), lit("-"), col("INTENT_APP")))).alias("PPM2"),
    spark_max(when((col("GROUP_NAME") == "preferred") & (col("PAYMENT_INDEX") == 2), concat(col("PAYMENT_NAME"), lit("-"), col("INTENT_APP")))).alias("PPM3"),
    spark_max(when((col("GROUP_NAME") == "single_click") & (col("PAYMENT_INDEX") == 0), col("PAYMENT_NAME"))).alias("PAYMENT_SHOWN_ON_TURBO")
)

# Write to intermediate Delta table
temp_table_2_name = "temp_5fe47e35e26cf162"
df_temp2_final.write.format("delta").mode("overwrite").saveAsTable(temp_table_2_name)

# ==============================================================================
# Step 4: Delete Operation
# ==============================================================================
# SQL: DELETE FROM ... WHERE (DT BETWEEN ...) OR (DT < START - 360)
target_table_name = "2f7c2b19e8d34ada.d9262e7fb868c502.fad3b36a65ae6b1f"
delete_condition = f"(DT BETWEEN '{start_date_val}' AND '{end_date_val}') OR (DT < date_sub('{start_date_val}', 360))"

# Using spark.sql for DELETE as it's the standard way for Delta tables
# Note: Using expressions inside f-string requires variable evaluation or proper formatting. 
# Since we are generating code, we assume the environment can run spark.sql with current logic.
# However, to be strictly pythonic variables, we can construct the string.

# We will perform delete via SQL command execution as Delta API delete requires condition building which is simpler in SQL string.
spark.sql(f"""
    DELETE FROM {target_table_name} 
    WHERE (DT BETWEEN date_sub(current_date(), 1) AND date_sub(current_date(), 1)) 
       OR (DT < date_sub(date_sub(current_date(), 1), 360))
""")

# ==============================================================================
# Step 5: Final Selection & Insert (Equivalent to ...b5b5b7)
# ==============================================================================

# 5.1 Prepare CTEs and Source Tables

# CHECKOUT CTE
df_checkout_source = spark.read.format("delta").table("64a52cf24d16c9c1")
df_checkout = df_checkout_source.alias("C").filter(
    col("DT").between(start_date_val, end_date_val)
).groupBy(
    col("DT"), col("USER_ID"), 
    get_json_object(col("BASIC_DATA"), "$.sid").cast("string").alias("SID_C"), 
    trim(lower(col("CITY"))).alias("CITY_NAME")
).count().select("DT", "USER_ID", "SID_C", "CITY_NAME") # Distinct via group by

# TURBO_CALLS_CARTS CTE (From Temp Table 1)
df_turbo_carts = spark.read.table(temp_table_1_name).filter(
    (col("S_FINALFLAG") == 1) & (col("PAGE") == "checkout")
).select(col("SID"))

# ORDER_DATA CTE
df_order_source = spark.read.format("delta").table("1e7dc6d6c1656540.66cbf792c934f76e") # Removed DB part
df_order_data = df_order_source.filter(
    (col("DT").between(start_date_val, end_date_val)) & 
    (col("IGNORE_ORDER_FLAG") == 0)
).select(
    col("DT"),
    col("SID").alias("SID_O"),
    col("POST_STATUS"),
    col("ORDER_ID").cast("string").alias("ORDER_ID"),
    upper(col("COMPOSITE_PAYMENT_METHOD")).alias("COMPOSITE_PAYMENT_METHOD"),
    upper(col("PAYMENT_GATEWAY")).alias("PAYMENT_GATEWAY"),
    col("RESTAURANT_ID"),
    col("CUST_PAYABLE"),
    col("ORDER_CREATED_TIME")
)

# CSV Data (PT)
# Assuming CSV is available at path 'dataoutput2.csv'. Adjust path as per env.
df_csv_pt = spark.read.option("header", "true").csv("dataoutput2.csv").alias("PT").select(
    col("DT"), 
    col("ORDER_ID"), 
    get_json_object(col("PAYMENT_DETAILS"), "$.omsOrderId").cast("string").alias("OMS_ORDER_ID")
).filter(col("DT").between(start_date_val, end_date_val))

# City Classification (CA)
df_city_ca = spark.read.format("delta").table("98b5843bc4ac4a3f").select(
    col("CLASSIFICATION"),
    trim(lower(col("CITY_NAME"))).alias("CITY_NAME"),
    col("LAUNCH_VERSION")
)

# Temp Table 1 (P) and Temp Table 2 (PA)
df_p_table = spark.read.table(temp_table_1_name).alias("P")
df_pa_table = spark.read.table(temp_table_2_name).alias("PA")

# 5.2 Joins
# Main driver is CHECKOUT (C)
df_main = df_checkout.alias("C") \
    .join(df_p_table, (col("C.SID_C") == col("P.SID")) & (col("C.DT") == col("P.DT")), "left") \
    .join(df_turbo_carts.alias("TURBO_CARTS"), col("C.SID_C") == col("TURBO_CARTS.SID"), "left") \
    .join(df_order_data.alias("O"), (col("P.SID") == col("O.SID_O")) & (col("C.DT") == col("O.DT")), "left") \
    .join(df_pa_table, col("PA.SID") == col("C.SID_C"), "left") \
    .join(df_csv_pt, (col("O.ORDER_ID") == col("PT.OMS_ORDER_ID")) & (col("O.DT") == col("PT.DT")), "left") \
    .join(df_city_ca.alias("CA"), col("C.CITY_NAME") == col("CA.CITY_NAME"), "left")

# 5.3 Projections and Transformations
df_final_select = df_main.select(
    col("C.DT"),
    col("C.USER_ID"),
    col("C.SID_C").alias("SID_C"),
    when(col("CA.CLASSIFICATION").isNotNull(), upper(col("CA.CLASSIFICATION")))
        .otherwise(coalesce(col("CA.CLASSIFICATION"), lit("UNCLASSIFIED CITY"))).alias("CITY_CLASSIFIER"),
    when(col("CA.LAUNCH_VERSION") == '1', upper(col("CA.CITY_NAME")))
        .otherwise("OTHER CITIES").alias("CITY_NAME"),
    when(col("TURBO_CARTS.SID").isNotNull(), 1).otherwise(0).alias("TURBO_SHOWN_ON_CART"),
    when(col("P.SID").isNotNull(), 1).otherwise(0).alias("IS_PAYMENT_SESSION"),
    
    when(col("P.SID").isNotNull(), col("P.TURBO_TYPE")).otherwise("NA").alias("TURBO_TYPE"),
    when(col("P.SID").isNotNull(), col("P.TURBO_FLAG")).otherwise("NA").alias("TURBO_FLAG"),
    when(col("P.SID").isNotNull(), col("P.REQUEST_ID").cast("string")).otherwise("NA").alias("REQUEST_ID"),
    
    when(col("P.SID").isNotNull(), col("P.P_SESSION_FLAG").cast("string")).otherwise("NA").alias("P_SESSION_FLAG"),
    when(col("P.SID").isNotNull(), col("P.PREFERRED_FLAG").cast("string")).otherwise("NA").alias("PREFERRED_FLAG"),
    
    when(col("PA.SID").isNotNull(), col("PA.PPM1").cast("string")).otherwise("NA").alias("PPM1"),
    when(col("PA.SID").isNotNull(), col("PA.PPM2").cast("string")).otherwise("NA").alias("PPM2"),
    when(col("PA.SID").isNotNull(), col("PA.PPM3").cast("string")).otherwise("NA").alias("PPM3"),
    when(col("PA.SID").isNotNull(), col("PA.PAYMENT_SHOWN_ON_TURBO").cast("string")).otherwise("NA").alias("PAYMENT_SHOWN_ON_TURBO"),
    
    when(col("O.SID_O").isNotNull(), 1).otherwise(0).alias("IS_ORDER_SESSION"),
    when(col("O.SID_O").isNotNull(), col("O.ORDER_ID").cast("string")).otherwise("NA").alias("ORDER_ID"),
    when(col("O.SID_O").isNotNull(), col("O.POST_STATUS").cast("string")).otherwise("NA").alias("POST_STATUS"),
    when(col("O.SID_O").isNotNull(), col("O.COMPOSITE_PAYMENT_METHOD").cast("string")).otherwise("NA").alias("COMPOSITE_PAYMENT_METHOD"),
    when(col("O.SID_O").isNotNull(), col("PT.ORDER_ID").cast("string")).otherwise("NA").alias("TRANSACTIN_ID"),
    when(col("O.SID_O").isNotNull(), col("O.RESTAURANT_ID").cast("string")).otherwise("NA").alias("RESTAURANT_ID")
)

# ==============================================================================
# Step 6: Insert into Target Table
# ==============================================================================

# Target table: 2f7c2b19e8d34ada.d9262e7fb868c502.fad3b36a65ae6b1f
target_table = "2f7c2b19e8d34ada.d9262e7fb868c502.fad3b36a65ae6b1f"

# 1. Extract target table schema
target_columns = [field.name for field in spark.table(target_table).schema]

# 2. Alias columns in source DataFrame to match target table columns (if necessary)
# In this specific query, the projection above sets the alias exactly as selected.
# We double check mapping. The source SQL uses explicit aliases which we followed.
# If implicit mapping is needed:
df_selected = df_final_select

# 3. Add missing columns with NULL values to match target table schema
for col_name in target_columns:
    if col_name not in df_selected.columns:
        df_selected = df_selected.withColumn(col_name, lit(None))

# 4. Reorder DataFrame columns to match target table schema
df_selected = df_selected.select(target_columns)

# 5. Insert the DataFrame into the target table
df_selected.write.mode("append").insertInto(target_table)