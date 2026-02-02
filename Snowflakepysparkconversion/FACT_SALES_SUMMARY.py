Here is the complete PySpark script converted from the provided Snowflake SQL query. This script follows the specific instructions for Databricks Serverless compatibility, strict table aliasing, and the robust `INSERT INTO` pattern.

from pyspark.sql.functions import (
    col, lit, when, trim, rtrim, ltrim, upper, lower, 
    substring, concat, sum, avg, max, coalesce, 
    regexp_replace, to_date, current_date, count
)

# ==========================================
# Step 1: Preparation & Load Tables
# ==========================================

# Load tables using Delta format as per instructions.
# Using full names (database.schema.table) in .table() and aliasing them immediately.

psa_df = spark.read.format("delta").table("ALCOBEVDB.ALCOBEV.PRIMARY_SALES_ACTUALS").alias("psa")
cm_df = spark.read.format("delta").table("ALCOBEVDB.ALCOBEV.COMPANYMASTER").alias("cm")
gm_df = spark.read.format("delta").table("ALCOBEVDB.ALCOBEV.GEOGRAPHYMASTER").alias("gm")
pm_df = spark.read.format("delta").table("ALCOBEVDB.ALCOBEV.PRODUCTMASTER").alias("pm")
psp_df = spark.read.format("delta").table("ALCOBEVDB.ALCOBEV.PRIMARY_SALES_PLAN_AOP").alias("psp")
om_df = spark.read.format("delta").table("ALCOBEVDB.ALCOBEV.OUTLETMASTER").alias("om")
am_df = spark.read.format("delta").table("ALCOBEVDB.ALCOBEV.ACTIVATIONMASTER").alias("am")

# ==========================================
# Step 2: Handle Subquery (amr)
# ==========================================
# Original SQL:
# (select PRODUCTLEVELCODE, max(EFFECTIVEFROM) ... group by PRODUCTLEVELCODE) amr

# We load the base table again for the subquery logic or derive from am_df (but carefully aliased)
# To match SQL exactly, we aggregate from ACTIVATIONMASTER.
amr_df = spark.read.format("delta").table("ALCOBEVDB.ALCOBEV.ACTIVATIONMASTER").alias("sub_am") \
    .groupBy(col("sub_am.PRODUCTLEVELCODE")) \
    .agg(max(col("sub_am.EFFECTIVEFROM")).alias("EFFECTIVEFROM")) \
    .alias("amr")

# ==========================================
# Step 3: Perform Joins
# ==========================================

# Start with base table `psa` and chain the joins
joined_df = psa_df \
    .join(cm_df, col("psa.COMPANYCODE") == col("cm.COMPANYCODE"), "left") \
    .join(gm_df, col("psa.STATECODE") == col("gm.STATECODE"), "left") \
    .join(pm_df, col("psa.SKUCODE") == col("pm.SKUCODE"), "left") \
    .join(psp_df, (col("psa.STATECODE") == col("psp.STATECODE")) & (col("psa.SKUCODE") == col("psp.SKUCODE")), "left") \
    .join(om_df, col("psa.CUSTOMERCODE") == col("om.OUTLET_CODE"), "left") \
    .join(am_df, col("psa.SKUCODE") == col("am.PRODUCTLEVELCODE"), "left") \
    .join(amr_df, (col("psa.SKUCODE") == col("amr.PRODUCTLEVELCODE")) & (col("am.EFFECTIVEFROM") == col("amr.EFFECTIVEFROM")), "left")

# ==========================================
# Step 4: Apply Filter (WHERE Clause)
# ==========================================

# SQL: coalesce(try_cast(LTRIM(RTRIM(psa.VOLUMEACTUALCASE)) as float), 0) > 0
# PySpark .cast("float") returns null on failure, acting like try_cast.
filter_condition = coalesce(trim(col("psa.VOLUMEACTUALCASE")).cast("float"), lit(0.0)) > 0
filtered_df = joined_df.filter(filter_condition)

# ==========================================
# Step 5: Define Column Transformations
# ==========================================

# We define expressions for the dimensions (Group By keys) first.
# Replicating logic: IFF(col is null OR col = 'NULL', 'default', transformation)

# COMPANYCODE
exp_companycode = when(
    col("psa.COMPANYCODE").isNull() | (col("psa.COMPANYCODE") == "NULL"), "UNKNOWN"
).otherwise(upper(trim(regexp_replace(col("psa.COMPANYCODE"), " ", ""))))

# COMPANYNAME
exp_companyname = when(
    col("cm.COMPANYNAME").isNull() | (col("cm.COMPANYNAME") == "NULL"), "UNKNOWN"
).otherwise(concat(substring(trim(col("cm.COMPANYNAME")), 1, 3), lit("***")))

# STATECODE
exp_statecode = when(
    col("psa.STATECODE").isNull() | (col("psa.STATECODE") == "NULL"), "UNKNOWN"
).otherwise(trim(col("psa.STATECODE")))

# STATENAME
exp_statename = when(
    col("gm.STATENAME").isNull() | (col("gm.STATENAME") == "NULL"), "UNKNOWN"
).otherwise(lower(trim(col("gm.STATENAME"))))

# SKUCODE
exp_skucode = when(
    col("psa.SKUCODE").isNull() | (col("psa.SKUCODE") == "NULL"), "UNKNOWN"
).otherwise(substring(trim(col("psa.SKUCODE")), 1, 10))

# ITEMNAME
exp_itemname = when(
    col("pm.ITEMNAME").isNull() | (col("pm.ITEMNAME") == "NULL"), "UNKNOWN"
).otherwise(regexp_replace(trim(col("pm.ITEMNAME")), " ", "_"))

# ACTIVE_FLAG
exp_active_flag = when(col("om.ACTIVE_FLAG") == 1, "YES").otherwise("NO")

# PROMOTION_DESC
exp_promotion_desc = when(
    col("am.PROMOTIONDESCRIPTION").isNull() | (col("am.PROMOTIONDESCRIPTION") == "NULL"), "NO PROMOTION"
).otherwise(concat(lit("Promo: "), trim(col("am.PROMOTIONDESCRIPTION"))))

# EFFECTIVE_DATE_RANGE_START
# Handling strings to date conversion. Spark to_date handles formatting.
exp_eff_start = when(
    col("am.EFFECTIVEFROM").isNull() | (col("am.EFFECTIVEFROM") == "NULL"), to_date(lit("1900-01-01"))
).otherwise(to_date(col("am.EFFECTIVEFROM"), "yyyy-MM-dd"))

# EFFECTIVE_DATE_RANGE_END
exp_eff_end = when(
    col("am.EFFECTIVETO").isNull() | (col("am.EFFECTIVETO") == "NULL"), current_date()
).otherwise(to_date(col("am.EFFECTIVETO"), "yyyy-MM-dd"))

# ==========================================
# Step 6: Apply Aggregations (GROUP BY)
# ==========================================

# In SQL, the GROUP BY clause repeats the transformation logic. 
# In PySpark, we project the dimensions first, then group by their aliases.

# Aggregation Expressions
# sum(coalesce(try_cast(TRIM(psa.VOLUMEACTUALCASE) as float), 0))
agg_total_vol = sum(coalesce(trim(col("psa.VOLUMEACTUALCASE")).cast("float"), lit(0.0)))

# sum(coalesce(try_cast(TRIM(psp.PLAN_QTY) as float), 0))
agg_total_plan = sum(coalesce(trim(col("psp.PLAN_QTY")).cast("float"), lit(0.0)))

# avg(coalesce(try_cast(TRIM(psa.VOLUMEACTUALCASE) as float), 0))
agg_avg_vol = avg(coalesce(trim(col("psa.VOLUMEACTUALCASE")).cast("float"), lit(0.0)))

# Create a DF with dimensions defined
pre_agg_df = filtered_df.select(
    exp_companycode.alias("COMPANYCODE"),
    exp_companyname.alias("COMPANYNAME"),
    exp_statecode.alias("STATECODE"),
    exp_statename.alias("STATENAME"),
    exp_skucode.alias("SKUCODE"),
    exp_itemname.alias("ITEMNAME"),
    exp_active_flag.alias("ACTIVE_FLAG"),
    exp_promotion_desc.alias("PROMOTION_DESC"),
    exp_eff_start.alias("EFFECTIVE_DATE_RANGE_START"),
    exp_eff_end.alias("EFFECTIVE_DATE_RANGE_END"),
    # Pass through columns needed for aggregation logic
    col("psa.VOLUMEACTUALCASE"),
    col("psp.PLAN_QTY")
)

# Perform Group By and Aggregation
result_df = pre_agg_df.groupBy(
    "COMPANYCODE",
    "COMPANYNAME",
    "STATECODE",
    "STATENAME",
    "SKUCODE",
    "ITEMNAME",
    "ACTIVE_FLAG",
    "PROMOTION_DESC",
    "EFFECTIVE_DATE_RANGE_START",
    "EFFECTIVE_DATE_RANGE_END"
).agg(
    agg_total_vol.alias("TOTAL_VOLUMEACTUALCASE"),
    agg_total_plan.alias("TOTAL_PLAN_QTY"),
    agg_avg_vol.alias("AVERAGE_VOLUME")
)

# ==========================================
# Step 7: Final Insert Logic (INSERT INTO)
# ==========================================

# Source DataFrame
employee_source_df = result_df

# Target table name
target_table = "FACT_SALES_SUMMARY"

# Extract target table schema
# Note: Ensure the target table exists in the Hive metastore/Delta location. 
# If not, create it first or write as 'overwrite'/'errorifexists' depending on requirements.
target_columns = [field.name for field in spark.table(target_table).schema]

# Alias columns in source DataFrame to match target table columns.
# Since we explicitly named columns in the aggregation step, they should mostly match.
# This step ensures safety.
aliased_columns = [
    col("COMPANYCODE").alias("COMPANYCODE"),
    col("COMPANYNAME").alias("COMPANYNAME"),
    col("STATECODE").alias("STATECODE"),
    col("STATENAME").alias("STATENAME"),
    col("SKUCODE").alias("SKUCODE"),
    col("ITEMNAME").alias("ITEMNAME"),
    col("TOTAL_VOLUMEACTUALCASE").alias("TOTAL_VOLUMEACTUALCASE"),
    col("TOTAL_PLAN_QTY").alias("TOTAL_PLAN_QTY"),
    col("AVERAGE_VOLUME").alias("AVERAGE_VOLUME"),
    col("ACTIVE_FLAG").alias("ACTIVE_FLAG"),
    col("PROMOTION_DESC").alias("PROMOTION_DESC"),
    col("EFFECTIVE_DATE_RANGE_START").alias("EFFECTIVE_DATE_RANGE_START"),
    col("EFFECTIVE_DATE_RANGE_END").alias("EFFECTIVE_DATE_RANGE_END")
]

# Create a new DataFrame with aliased columns
df_selected = employee_source_df.select(*aliased_columns)

# Add missing columns with NULL values to match target table schema
for col_name in target_columns:
    if col_name not in df_selected.columns:
        df_selected = df_selected.withColumn(col_name, lit(None))

# Reorder DataFrame columns to match target table schema
df_selected = df_selected.select(target_columns)

# Insert the DataFrame into the target table
# Using append mode as per INSERT INTO semantics
if not df_selected.isEmpty():
    df_selected.write.format("delta").mode("append").insertInto(target_table)