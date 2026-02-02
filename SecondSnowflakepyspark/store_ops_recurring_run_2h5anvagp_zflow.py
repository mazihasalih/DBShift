import pyspark.sql.functions as F
from pyspark.sql.functions import (
    col, lit, when, explode, from_json, get_json_object, 
    current_date, date_sub, date_add, hour, to_timestamp, 
    to_date, concat, substring, avg, sum as spark_sum, 
    max as spark_max, min as spark_min, coalesce, count, 
    row_number, last, dayofweek, split, round, dense_rank,
    last_day, month, weekofyear, abs as spark_abs
)
from pyspark.sql.types import ArrayType, StringType, IntegerType, DoubleType, MapType
from pyspark.sql.window import Window

# Helper function to handle INSERT operations as per instructions
def insert_into_target(spark, source_df, target_table, mode="append"):
    """
    Handles INSERT INTO logic: 
    1. Align source columns with target schema.
    2. Add missing columns as NULL.
    3. Reorder columns.
    4. Write to target.
    """
    try:
        # Step 1: Extract target table schema
        target_columns = [field.name for field in spark.table(target_table).schema]
        
        # Step 2: Alias columns (Assumption: logic already produces correct aliased names, 
        # but we ensure selection based on target columns existing in source)
        # In this script, we ensure the transformations produce the aliased names expected.
        
        # Step 3: Add missing columns with NULL
        df_selected = source_df
        for col_name in target_columns:
            if col_name not in df_selected.columns:
                df_selected = df_selected.withColumn(col_name, lit(None))
        
        # Step 4: Reorder
        df_selected = df_selected.select(target_columns)
        
        # Step 5: Insert
        df_selected.write.format("delta").mode(mode).insertInto(target_table)
        print(f"Successfully inserted into {target_table}")
        
    except Exception as e:
        print(f"Error inserting into {target_table}: {str(e)}")
        raise e

# ==============================================================================
# BLOCK 1: Processing b969079f88836732.d9262e7fb868c502.297051a6397167f5
# ==============================================================================

# DELETE logic
target_1 = "b969079f88836732.d9262e7fb868c502.297051a6397167f5"
spark.sql(f"DELETE FROM {target_1} WHERE dt >= current_date() - 1")

# INSERT logic
# Source: 6754af9632a2745e.a2064f061d17278b.2007ffc9d8bfdd2d
df_source_1 = spark.read.format("delta").table("6754af9632a2745e.a2064f061d17278b.2007ffc9d8bfdd2d")

# Filter dates
df_source_1 = df_source_1.filter(
    (col("dt") >= date_sub(current_date(), 1)) & 
    (col("dt") <= current_date())
)

# Lateral Flatten equivalent: Explode itemdetails
# Assuming itemdetails is a JSON string representing an array of objects
# We interpret 'value' from flatten as the exploded item
df_flattened_1 = df_source_1.select(
    col("dt"),
    col("orderjobid"),
    col("timestamp"),
    explode(from_json(col("itemdetails"), ArrayType(StringType()))).alias("value")
)

# Extract fields
df_extracted_1 = df_flattened_1.select(
    col("dt"),
    col("orderjobid"),
    get_json_object(col("value"), "$.perishable").alias("Temp_sens"),
    hour((col("timestamp") / 1000).cast("timestamp")).alias("order_hour")
)

# Logic to prioritize 'true' over 'false' for Temp_sens per orderjobid
# cte1: temp_sens = 'true'
cte1 = df_extracted_1.filter(col("Temp_sens") == 'true')

# cte2: temp_sens = 'false'
cte2 = df_extracted_1.filter(col("Temp_sens") == 'false')

# cte3: in cte2 but not in cte1 (based on orderjobid)
cte3 = cte2.join(cte1.select("orderjobid"), on="orderjobid", how="left_anti")

# Union
df_final_1 = cte1.union(cte3).select("DT", "TEMP_SENS", "ORDERJOBID")

insert_into_target(spark, df_final_1, target_1)


# ==============================================================================
# BLOCK 2: Processing 65f98121a162a56a.efa1f375d76194fa.1ff838289e3b6ee4
# ==============================================================================

target_2 = "65f98121a162a56a.efa1f375d76194fa.1ff838289e3b6ee4"
# DELETE (Truncate equivalent)
spark.sql(f"DELETE FROM {target_2}")

# INSERT
df_source_2 = spark.read.format("delta").table("6754af9632a2745e.a2064f061d17278b.2007ffc9d8bfdd2d")

df_final_2 = df_source_2.filter(
    (col("dt") == current_date()) &
    F.lower(col("state")).isin('picking','picked','delivery_ready','cancelled','assigned','un_assigned')
).select(
    col("orderjobid"),
    col("slotted"),
    col("timestamp"),
    F.lower(col("state")).alias("state1"),
    col("dt"),
    col("storeid"),
    col("metadata"),
    col("time_stamp")
)

insert_into_target(spark, df_final_2, target_2)


# ==============================================================================
# BLOCK 3: Processing 65f98121a162a56a.efa1f375d76194fa.e229431168cf9039
# ==============================================================================

target_3 = "65f98121a162a56a.efa1f375d76194fa.e229431168cf9039"
spark.sql(f"DELETE FROM {target_3}")

df_source_3 = spark.read.format("delta").table("6754af9632a2745e.a2064f061d17278b.75460396915f2efe")

df_final_3 = df_source_3.filter(
    (col("dt") == current_date()) &
    F.lower(col("status")).isin('delivery_pickedup','delivery_arrived')
).select(
    col("job_id"),
    col("time_stamp"),
    F.lower(col("status")).alias("status1")
)

insert_into_target(spark, df_final_3, target_3)


# ==============================================================================
# BLOCK 4: Processing a6864eb339b0e1f6.efa1f375d76194fa.59d3d2e5a7291554
# ==============================================================================
# This is a complex transformation involving multiple joins and aggregations.

target_4 = "a6864eb339b0e1f6.efa1f375d76194fa.59d3d2e5a7291554"
spark.sql(f"DELETE FROM {target_4} WHERE dt = current_date()")

# Load base tables for Block 4
# Note: Using the tables populated in previous steps (target_2, target_3) as sources as per SQL logic
df_1ff8 = spark.read.format("delta").table("65f98121a162a56a.efa1f375d76194fa.1ff838289e3b6ee4")
df_pigeon = spark.read.format("delta").table("6754af9632a2745e.a2064f061d17278b.47ab24519425d65d") # quoted in sql
df_store_events = spark.read.format("delta").table("6754af9632a2745e.a2064f061d17278b.bd7cc17e0b98e0b5")
df_orders = spark.read.format("delta").table("6754af9632a2745e.a2064f061d17278b.639f449b88b40065")
df_instacart_types = spark.read.format("delta").table("2f7c2b19e8d34ada.d9262e7fb868c502.edaa44eba63b76d1")
df_e229 = spark.read.format("delta").table("65f98121a162a56a.efa1f375d76194fa.e229431168cf9039")
df_2007 = spark.read.format("delta").table("6754af9632a2745e.a2064f061d17278b.2007ffc9d8bfdd2d") # quoted in sql
df_temp_sens = spark.read.format("delta").table("a6864eb339b0e1f6.efa1f375d76194fa.297051a6397167f5")

# Sub-query A: Vendor Picking Time
a_df = df_1ff8.filter(col("state") == 'picking') \
    .groupBy("orderjobid", "slotted") \
    .agg(spark_max((col("timestamp") / 1000).cast("timestamp")).alias("vendor_picking_time"))

# Sub-query B: Vendor Picked Time
b_df = df_1ff8.filter(col("state") == 'picked') \
    .groupBy("orderjobid") \
    .agg(spark_max((col("timestamp") / 1000).cast("timestamp")).alias("vendor_picked_time"))

# Sub-query C: Vendor Marked Ready
c_df = df_1ff8.filter(col("state") == 'delivery_ready') \
    .groupBy("orderjobid") \
    .agg(spark_max((col("timestamp") / 1000).cast("timestamp")).alias("vendor_markedready_time"))

# Sub-query D: Vendor Cancelled
d_df = df_1ff8.filter(col("state") == 'cancelled') \
    .groupBy("orderjobid") \
    .agg(spark_max((col("timestamp") / 1000).cast("timestamp")).alias("vendor_cancelled_time"))

# Sub-query E: Vendor Assigned
e_df = df_1ff8.filter(col("state") == 'assigned') \
    .groupBy("orderjobid") \
    .agg(spark_min((col("timestamp") / 1000).cast("timestamp")).alias("vendor_assigned_time"))

# Sub-query F: Pigeon Hole
f_df = df_pigeon.filter(
    (col("object_name") == 'place-order-in -pigeon-hole') & 
    (col("dt") == current_date())
).groupBy("object_value") \
 .agg(spark_max(to_timestamp(col("TIME_STAMP"))).alias("vendor_ph_time"))

# Sub-query G: Unassigned Flag
g_df = df_1ff8.filter(col("state") == 'un_assigned').select("orderjobid").distinct() \
    .withColumn("unassigned_flag", lit(1)) # Logic simplified based on SQL distinct selection

# Sub-query H: Store Info
# Window for last_value
w_h = Window.partitionBy("id").orderBy("event_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

h_df_raw = df_store_events.filter(
    col("job_type").isin('BUY','CUSTOM','instacart') &
    (to_date(col("dt")) == current_date())
).withColumn("date", to_date((col("created_at") / 1000).cast("timestamp")))

# Extract JSON fields safely
h_df_raw = h_df_raw.withColumn("storeType", get_json_object(col("metadata_json"), "$.storeInfo.storeType"))
h_df_raw = h_df_raw.withColumn("storeType_clean", F.lower(F.regexp_replace(col("storeType"), '"', '')))

h_df_filtered = h_df_raw.filter(
    (col("storeType_clean") == 'listed') | col("storeType_clean").isNull()
).withColumn("last_event_id", F.last("event_id").over(w_h))

h_df = h_df_filtered.filter(col("event_id") == col("last_event_id")).groupBy("id", "status", "dt").agg(
    spark_max("city").alias("city"),
    spark_max(get_json_object(col("metadata_json"), "$.storeInfo.id")).alias("store_id"),
    spark_max(F.regexp_replace(get_json_object(col("metadata_json"), "$.storeInfo.name"), '"', '')).alias("store_name")
)

# Sub-query M: Ordered Time
m_df = df_orders.filter(
    (col("dt") == current_date()) &
    col("type").isin('BUY','CUSTOM','instacart','HANDPICKED') &
    (col("status") == 'CONFIRMED')
).groupBy("job_id").agg(
    spark_min(substring((col("time_stamp") / 1000).cast("timestamp").cast("string"), 1, 19).cast("timestamp")).alias("ordered_time")
)

# Sub-query Z: Instacart Type
z_df = df_instacart_types.filter(col("type") == 'instacart').select(
    "city", "store_id", F.lower(col("store_name")).alias("type")
)

# Sub-query I: Delivery Arrived
i_df = df_e229.filter(col("status") == 'delivery_arrived').groupBy("job_id").agg(
    spark_min((col("time_stamp") / 1000).cast("timestamp")).alias("de_arrived")
)

# Sub-query J: Delivery Picked
j_df = df_e229.filter(col("status") == 'delivery_pickedup').groupBy("job_id").agg(
    spark_min((col("time_stamp") / 1000).cast("timestamp")).alias("de_picked")
)

# Sub-query K: FTR/Qty Diff
# This involves complex nested JSON parsing and comparison
k_base = df_2007.filter((col("state") == 'PICKED') & (col("dt") == current_date()))
w_k = Window.partitionBy("ORDERJOBID").orderBy((col("timestamp")/1000).cast("timestamp"))
k_ranked = k_base.withColumn("urank", row_number().over(w_k)).filter(col("urank") == 1)

# Processing Old Items
k_old_explode = k_ranked.select(
    "orderjobid", 
    explode(from_json(get_json_object(col("metadata"), "$.oldItems"), ArrayType(StringType()))).alias("value")
)
k_old_agg = k_old_explode.select(
    "orderjobid",
    get_json_object(col("value"), "$.skuId").alias("sku_old"),
    get_json_object(col("value"), "$.quantity").cast("double").alias("quantity")
).groupBy("orderjobid", "sku_old").agg(spark_sum("quantity").alias("old_quantity"))

# Processing New Items
k_new_explode = k_ranked.select(
    "orderjobid", 
    explode(from_json(get_json_object(col("metadata"), "$.newItems"), ArrayType(StringType()))).alias("value")
)
k_new_agg = k_new_explode.select(
    "orderjobid",
    get_json_object(col("value"), "$.skuId").alias("sku_new"),
    get_json_object(col("value"), "$.quantity").cast("double").alias("quantity")
).groupBy("orderjobid", "sku_new").agg(spark_sum("quantity").alias("new_quantity"))

# Joining Old and New to calculate diff
k_joined = k_old_agg.join(
    k_new_agg, 
    (k_old_agg.orderjobid == k_new_agg.orderjobid) & (k_old_agg.sku_old == k_new_agg.sku_new),
    "left"
)
k_diff = k_joined.withColumn(
    "qty_diff", col("old_quantity") - coalesce(col("new_quantity"), lit(0))
).filter(col("qty_diff") > 0)

k_df = k_diff.groupBy(k_old_agg.orderjobid).agg(
    lit('1').alias("ftr_flag"),
    spark_sum("qty_diff").alias("qty_diff")
)

# Main Join Sequence for CTE in Block 4
# Start with 'a' (Picking Time)
main_df = a_df.alias("a") \
    .join(b_df.alias("b"), col("a.orderjobid") == col("b.orderjobid"), "left") \
    .join(c_df.alias("c"), col("a.orderjobid") == col("c.orderjobid"), "left") \
    .join(d_df.alias("d"), col("a.orderjobid") == col("d.orderjobid"), "left") \
    .join(e_df.alias("e"), col("a.orderjobid") == col("e.orderjobid"), "left") \
    .join(f_df.alias("f"), col("a.orderjobid") == col("f.object_value"), "left") \
    .join(g_df.alias("g"), col("a.orderjobid") == col("g.orderjobid"), "left") \
    .join(h_df.alias("h"), col("a.orderjobid") == col("h.id"), "left") \
    .join(m_df.alias("m"), col("a.orderjobid") == col("m.job_id"), "left") \
    .join(z_df.alias("z"), col("h.store_id") == col("z.store_id"), "left") \
    .join(i_df.alias("i"), col("a.orderjobid") == col("i.job_id"), "left") \
    .join(j_df.alias("j"), col("a.orderjobid") == col("j.job_id"), "left") \
    .join(k_df.alias("k"), col("a.orderjobid") == col("k.orderjobid"), "left")

# Apply Filter: lower(h.store_name) in ('instacart')
main_df = main_df.filter(F.lower(col("h.store_name")) == 'instacart')

# Select columns for the CTE projection
cte_result = main_df.select(
    col("a.orderjobid"),
    col("a.slotted"),
    col("a.vendor_picking_time").alias("vendor_pickingstart_time"),
    col("b.vendor_picked_time"),
    col("c.vendor_markedready_time"),
    col("d.vendor_cancelled_time"),
    col("e.vendor_assigned_time"),
    col("f.vendor_ph_time"),
    col("g.unassigned_flag"),
    col("m.ordered_time"),
    col("z.city"),
    to_date(col("m.ordered_time")).alias("dt"),
    col("h.store_id").cast("string").alias("store_id"),
    col("h.store_name"),
    col("h.status"),
    col("i.de_arrived"),
    col("j.de_picked"),
    coalesce(col("k.ftr_flag"), lit(0)).alias("ftr_flag"),
    coalesce(col("k.qty_diff"), lit(0)).alias("qty_diff"),
    lit(None).alias("igcc_flag"),
    lit(None).alias("missing_item_igcc"),
    lit(None).alias("wrong_item_igcc"),
    lit(None).alias("packaging_igcc"),
    lit(None).alias("quality_fnv_igcc"),
    lit(None).alias("quality_nonfnv_igcc"),
    lit(None).alias("wrong_order_igcc"),
    lit(None).alias("missing_order_igcc"),
    when(F.lower(col("h.status")) == 'delivery_delivered', 1).otherwise(0).alias("del_flag")
)

# Apply final CTE transformations (timestamp diffs)
cte_calculated = cte_result.withColumn("o2a", (col("vendor_assigned_time").cast("long") - col("ordered_time").cast("long")) / 60) \
    .withColumn("a2c", (col("vendor_pickingstart_time").cast("long") - col("vendor_assigned_time").cast("long")) / 60) \
    .withColumn("c2p", (col("vendor_picked_time").cast("long") - col("vendor_pickingstart_time").cast("long")) / 60) \
    .withColumn("p2b", (col("vendor_markedready_time").cast("long") - col("vendor_picked_time").cast("long")) / 60) \
    .withColumn("o2p", (col("vendor_pickingstart_time").cast("long") - col("ordered_time").cast("long")) / 60) \
    .withColumn("o2b", (col("vendor_markedready_time").cast("long") - col("ordered_time").cast("long")) / 60) \
    .withColumn("b2ar", (col("de_arrived").cast("long") - col("vendor_markedready_time").cast("long")) / 60) \
    .withColumn("ar2p", (col("de_picked").cast("long") - col("de_arrived").cast("long")) / 60) \
    .withColumn("hr", hour(col("ordered_time")))

# Prepare temp_sens subquery for final join
b_sub = df_temp_sens.select("orderjobid", "temp_sens").distinct()

# Final select for Block 4
df_final_4 = cte_calculated.alias("a").join(
    b_sub.alias("b"), col("a.orderjobid") == col("b.orderjobid"), "left"
).select(
    col("a.*"), col("b.temp_sens")
)

insert_into_target(spark, df_final_4, target_4)

# ==============================================================================
# BLOCK 5: Processing 65f98121a162a56a.efa1f375d76194fa.56269461814d9e29
# ==============================================================================

target_5 = "65f98121a162a56a.efa1f375d76194fa.56269461814d9e29"
spark.sql(f"DELETE FROM {target_5}")

df_3b9c = spark.read.format("delta").table("a6864eb339b0e1f6.efa1f375d76194fa.3b9cf97645e80eb9")
df_9c37 = spark.read.format("delta").table("65f98121a162a56a.efa1f375d76194fa.9c378d0a2ffdaf82")
df_bus_raw = spark.read.format("delta").table("cef84c440aef9bd5.0ff26e54712e66d6.68d47a7a0b4c2ac4")
df_cap_raw = spark.read.format("delta").table("6754af9632a2745e.0a5765423c8374e5.1e34b56e985c5ccf")

# Base CTE
# Cross join logic
base_a = df_3b9c.select(col("hour").alias("hr"))
base_b = df_9c37.filter((F.lower(col("store_name")) == 'instacart') & (col("active") == 1)) \
    .select("city", col("store_id").cast("string").alias("store_id"))
base = base_a.crossJoin(base_b).withColumn("day_id", dayofweek(current_date()).cast("string")) \
    .withColumn("current_day_type", when(F.date_format(current_date(), 'E').isin('Sat', 'Sun'), 'weekend').otherwise('weekday'))

# Bus_hr CTE
stores_for_bus = base_b.select("store_id").distinct()
bus_hr = df_bus_raw.join(stores_for_bus, df_bus_raw.store_id.cast("string") == stores_for_bus.store_id, "inner") \
    .filter(col("deleted_time").isNull()) \
    .select(
        col("day_id").cast("string"),
        col("store_id").cast("string"),
        split(col("open_time"), ':')[0].cast("integer").alias("open_hr"),
        when(
            (split(col("close_time"), ':')[1].cast("integer") == 0) & 
            (split(col("close_time"), ':')[2].cast("integer") == 0),
            split(col("close_time"), ':')[0].cast("integer") - 1
        ).otherwise(split(col("close_time"), ':')[0].cast("integer")).alias("close_hr")
    )

# Raw CTE
raw = df_cap_raw.select(
    col("storeid").alias("store_id"),
    col("enabled"),
    col("threshold").alias("capacity"),
    col("cooldown"),
    col("STRESSRATIOTHRESHOLD").alias("capacity_mul"),
    col("STRESSRATIOCOOLDOWN").alias("cooldown_mul"),
    col("slot"),
    split(col("slot"), '-')[0].alias("start_time"),
    split(col("slot"), '-')[1].alias("end_time"),
    split(col("slot"), '-')[2].alias("day_type")
)

# Non_default CTE
current_day_types = base.select("current_day_type").distinct()
non_default = raw.filter(
    (~col("start_time").isin('default', 'mfr'))
).join(current_day_types, raw.day_type == current_day_types.current_day_type, "inner") \
.select(
    "store_id", "capacity", "cooldown", "capacity_mul", "cooldown_mul", "slot",
    split(col("start_time"), ':')[0].cast("integer").alias("start_time"),
    split(col("end_time"), ':')[0].cast("integer").alias("end_time"),
    "day_type"
)

# Default_cap CTE
default_cap = raw.filter(col("start_time").isin('default', 'mfr')).select(
    "capacity", "cooldown", "capacity_mul", "cooldown_mul", "slot", "enabled", "start_time",
    when(col("slot") == 'default', col("store_id")).otherwise(split(col("store_id"), '_')[2]).alias("storeid")
)

# Pre_final CTE
pre_final = base.alias("a") \
    .join(non_default.alias("b"), (col("a.hr") == col("b.start_time")) & (col("a.store_id") == col("b.store_id")), "left") \
    .join(default_cap.alias("c"), col("a.store_id") == col("c.storeid"), "left") \
    .filter((col("enabled") == 'true') | col("enabled").isNull()) \
    .select(
        col("a.*"),
        when(col("c.start_time") == 'mfr', 'RFD').otherwise('Pre-MFR').alias("capacity_type"),
        when(col("c.start_time") == 'mfr', col("c.capacity")).otherwise(coalesce(col("b.capacity"), col("c.capacity"))).alias("capacity"),
        when(col("c.start_time") == 'mfr', col("c.cooldown")).otherwise(coalesce(col("b.cooldown"), col("c.cooldown"))).alias("cooldown"),
        when(col("c.start_time") == 'mfr', col("c.capacity_mul")).otherwise(coalesce(col("b.capacity_mul"), col("c.capacity_mul"))).alias("capacity_mul"),
        when(col("c.start_time") == 'mfr', col("c.cooldown_mul")).otherwise(coalesce(col("b.cooldown_mul"), col("c.cooldown_mul"))).alias("cooldown_mul")
    )

# Final CTE
final_5 = pre_final.alias("a").join(
    bus_hr.alias("b"),
    (col("a.day_id") == col("b.day_id")) & 
    (col("a.store_id") == col("b.store_id")) & 
    (col("a.hr").between(col("b.open_hr"), col("b.close_hr")))
).select(
    col("a.city"),
    col("a.store_id").alias("storeid"),
    col("a.hr"),
    col("a.day_id"),
    col("a.current_day_type"),
    col("a.capacity_type"),
    col("a.capacity").alias("threshold"),
    col("a.cooldown"),
    col("a.capacity_mul").alias("threshold_mul"),
    col("a.cooldown_mul")
)

insert_into_target(spark, final_5, target_5)

# ==============================================================================
# BLOCK 6: Processing 65f98121a162a56a.efa1f375d76194fa.c64266041c7eb60b
# ==============================================================================

target_6 = "65f98121a162a56a.efa1f375d76194fa.c64266041c7eb60b"
spark.sql(f"DELETE FROM {target_6}")

df_7546 = spark.read.format("delta").table("6754af9632a2745e.a2064f061d17278b.75460396915f2efe")

df_final_6 = df_7546.filter(
    (col("dt") == current_date()) & 
    F.lower(col("status")).isin('delivery_pickedup','delivery_arrived','placed','delivery_delivered','cancelled')
).select(
    col("job_id"),
    col("time_stamp"),
    F.lower(col("status")).alias("status1"),
    col("dt"),
    col("store_id"),
    col("update_time"),
    col("update_json")
)

insert_into_target(spark, df_final_6, target_6)

# ==============================================================================
# BLOCK 7: Processing 65f98121a162a56a.efa1f375d76194fa.3fb7dfad6a3e7593
# ==============================================================================

target_7 = "65f98121a162a56a.efa1f375d76194fa.3fb7dfad6a3e7593"
spark.sql(f"DELETE FROM {target_7}")

# Load Tables
df_stores_a = spark.read.format("delta").table("98b06824c41bbe8e.1d0a85e7bcc3606f.4e7cf3cf5960fe13")
df_stores_b = spark.read.format("delta").table("98b06824c41bbe8e.1d0a85e7bcc3606f.4a91ee5f0106c2b3")
df_stores_c = spark.read.format("delta").table("98b06824c41bbe8e.1d0a85e7bcc3606f.11a62c23412b7747")
df_sku_spin = spark.read.format("delta").table("65f98121a162a56a.efa1f375d76194fa.713ac56e25cb85a7")
df_ftr_items = spark.read.format("delta").table("65f98121a162a56a.efa1f375d76194fa.1ff838289e3b6ee4")
df_order_logs = spark.read.format("delta").table("65f98121a162a56a.efa1f375d76194fa.c64266041c7eb60b") # populated in target_6
df_closing = spark.read.format("delta").table("65f98121a162a56a.efa1f375d76194fa.5c04c59e5aee2c1e")
df_current_stock = spark.read.format("delta").table("6754af9632a2745e.c69fa29d1c1d7f2f.b401bc9d69192265")
df_item_names = spark.read.format("delta").table("65f98121a162a56a.efa1f375d76194fa.a8eb7ae446365afd")

# Stores Det
stores_det = df_stores_a.alias("a") \
    .join(df_stores_b.alias("b"), col("a.area_id") == col("b.id"), "left") \
    .join(df_stores_c.alias("c"), col("c.id") == col("b.city_id"), "left") \
    .filter(F.lower(col("a.name")) == 'instacart') \
    .groupBy(col("a.id").alias("STORE_ID"), col("b.name").alias("area"), col("c.name").alias("city"), F.lower(col("a.name")).alias("store_name")) \
    .count().drop("count") # Distinct equivalent

# SKU Spin
sku_spin_base = df_sku_spin.withColumn("no_items", count("const_sku").over(Window.partitionBy("store_id", "combo_sku"))) \
    .withColumn("combo_type", when(col("no_items") > 1, "hetero").otherwise("homo"))

sku_spin2 = sku_spin_base.alias("a").join(stores_det.alias("b"), col("a.store_id") == col("b.store_id")) \
    .select(
        col("combo_spin").alias("combo_spin_id"),
        col("const_sku").alias("const_sku_id"),
        col("a.store_id"),
        col("combo_sku").alias("combo_sku_id"),
        col("combo_type"),
        col("const_qty")
    )

# FTR Items
ftr_items = df_ftr_items.alias("a").join(stores_det.alias("b"), col("a.storeid") == col("b.store_id")) \
    .filter((col("dt") == current_date()) & (col("state") == 'picked')) \
    .select(
        "dt", col("storeid").alias("store_id"), "state", "orderjobid", "metadata", "time_stamp",
        when(col("slotted") == 'TRUE', 'SLOTTED').otherwise('INSTANT').alias("delivery_type")
    )

# Old Items (Explode)
old = ftr_items.select(
    "dt", "store_id", "state", "orderjobid", "delivery_type", "time_stamp",
    explode(from_json(get_json_object(col("metadata"), "$.oldItems"), ArrayType(StringType()))).alias("value")
).select(
    "dt", "store_id", "state", "orderjobid", "delivery_type",
    get_json_object(col("value"), "$.skuId").alias("sku_id"),
    get_json_object(col("value"), "$.quantity").cast("integer").alias("ordered_qty"),
    ((col("time_stamp") / 1000).cast("timestamp")).alias("vdc_ftr_time")
)

# New Items (Explode)
new = ftr_items.select(
    "dt", "store_id", "state", "orderjobid", "delivery_type", "time_stamp",
    explode(from_json(get_json_object(col("metadata"), "$.newItems"), ArrayType(StringType()))).alias("value")
).select(
    "dt", "store_id", "state", "orderjobid", "delivery_type",
    get_json_object(col("value"), "$.skuId").alias("sku_id"),
    get_json_object(col("value"), "$.quantity").cast("integer").alias("new_qty"),
    ((col("time_stamp") / 1000).cast("timestamp")).alias("vdc_ftr_time")
)

# Old New Join
old_new = old.alias("a").join(new.alias("b"), 
    (col("a.orderjobid") == col("b.orderjobid")) & (col("a.sku_id") == col("b.sku_id")), "left"
).select(
    col("a.*"),
    coalesce(col("b.new_qty"), lit(0)).alias("new_qty"),
    lit('FTR').alias("reason"),
    when(col("a.ordered_qty") - coalesce(col("b.new_qty"), lit(0)) != 0, 'FTR').otherwise('non-ftr').alias("vdc_ftr_flag")
)

# Placed Orders
placed_orders = df_order_logs.alias("a").join(stores_det.alias("b"), col("a.store_id") == col("b.store_id")) \
    .filter((col("dt") == current_date()) & (col("status1") == 'placed')) \
    .groupBy("dt", "a.store_id", col("job_id").alias("orderjobid"), col("status1").alias("status")) \
    .agg(spark_min("update_time").alias("ordered_time"))

# Del Orders
del_orders = df_order_logs.alias("a").join(placed_orders.alias("b"), col("a.job_id") == col("b.orderjobid")) \
    .filter((col("a.dt") == current_date()) & (col("status1") == 'delivery_delivered')) \
    .groupBy("a.dt", "a.store_id", col("job_id").alias("orderjobid"), col("status1").alias("status")) \
    .agg(spark_min("update_time").alias("delivery_time"))

# Cancelled Orders
# Parsing complex JSON in 'update_json'
# Path: update_json -> status_meta -> reason
# Path: update_json -> metadata -> bill -> billedItems (array)
canc_orders_base = df_order_logs.alias("a").join(placed_orders.alias("b"), col("a.job_id") == col("b.orderjobid")) \
    .filter((col("a.dt") == current_date()) & (col("status1") == 'cancelled'))

canc_flatten = canc_orders_base.select(
    col("a.dt"), col("a.store_id"), col("job_id").alias("orderjobid"), col("status1").alias("status"),
    col("update_time"), col("update_json"),
    get_json_object(get_json_object(col("update_json"), "$.status_meta"), "$.reason").alias("reason"),
    get_json_object(get_json_object(col("update_json"), "$.metadata"), "$.deliveryType").alias("delivery_type"),
    explode(from_json(get_json_object(get_json_object(get_json_object(col("update_json"), "$.metadata"), "$.bill"), "$.billedItems"), ArrayType(StringType()))).alias("k_val")
)

canc_reasons = [
    '%DE Issue%', '%Order Item(s) not available%', '%Stores_VDC | VDC_DE found store closed%',   
	'%Stores_VDC | VDC_vendor app issue%', '%Stores_VDC | VDC_OOS%','%Stores_VDC | Others%',                                        
	'%Stores_VDC| VDC_incorrect OOS%', '%Delivery Executive crowding at store%',                                        
	'%Unable to service online orders%', '%Closing my store%', '%Customer rush at store%',                                        
	'%Stores_VDC | VDC_incorrect store location%', '%Stores_VDC | VDC_bill difference%'
]
# Spark SQL 'like any' equivalent: OR chain
reason_condition = F.lit(False)
for r in canc_reasons:
    reason_condition = reason_condition | col("reason").like(r)

canc_orders = canc_flatten.filter(reason_condition).select(
    "dt", "store_id", "orderjobid", "status", "reason", "delivery_type",
    get_json_object(col("k_val"), "$.item.itemId").alias("sku_id"),
    get_json_object(col("k_val"), "$.item.quantity").cast("integer").alias("ordered_qty"),
    lit('VDC').alias("vdc_ftr_flag"),
    col("update_time").alias("vdc_ftr_time")
).groupBy("dt", "store_id", "orderjobid", "status", "sku_id", "ordered_qty", "reason", "delivery_type", "vdc_ftr_flag") \
.agg(spark_min("vdc_ftr_time").alias("vdc_ftr_time"))

# FTR Orders
ftr_orders = old_new.alias("a").join(del_orders.alias("b"), col("a.orderjobid") == col("b.orderjobid")) \
    .filter(col("vdc_ftr_flag") == 'FTR') \
    .select("a.dt", "a.store_id", "a.orderjobid", "sku_id", "ordered_qty", "delivery_type", "vdc_ftr_time", "reason", "vdc_ftr_flag")

# Union FTR and Cancelled
vdc_ftr_orders = ftr_orders.select("dt", "store_id", "orderjobid", col("sku_id").alias("temp_sku"), "ordered_qty", "delivery_type", "vdc_ftr_time", "reason", "vdc_ftr_flag") \
    .union(canc_orders.select("dt", "store_id", "orderjobid", col("sku_id").alias("temp_sku"), "ordered_qty", "delivery_type", "vdc_ftr_time", "reason", "vdc_ftr_flag"))

# Final Orders Logic
vdc_ftr_orders_final = vdc_ftr_orders.alias("a").join(sku_spin2.alias("b"), col("a.temp_sku") == col("b.combo_sku_id"), "left") \
    .select(
        col("a.*"),
        coalesce(col("b.const_sku_id"), col("temp_sku")).alias("sku_id"),
        col("b.const_qty"),
        (col("ordered_qty") * coalesce(col("b.const_qty"), lit(1))).alias("final_ordered_qty"),
        when(col("const_sku_id").isNotNull(), col("temp_sku")).alias("combo_sku_id")
    )

# Inventory logic
closing_stock = df_closing.filter(
    (col("insert_date") == date_add(current_date(), -2)) & (col("loc") != '#inwarding_area')
).groupBy("sku").agg(
    spark_sum("sellable").alias("sellable_units"),
    spark_max("update_time").alias("update_time")
).withColumn("update_date", to_date("update_time"))

curr_stock = df_current_stock.filter(
    to_date((col("sellable_updated") / 1000).cast("timestamp")).between(date_add(current_date(), -2), current_date())
).select(
    "sku",
    (col("sellable") - coalesce(col("ordered"), lit(0))).alias("sellable_units"),
    (col("sellable_updated") / 1000).cast("timestamp").alias("update_time")
).withColumn("update_date", to_date("update_time"))

inv = closing_stock.select("sku", "sellable_units", "update_time", "update_date") \
    .union(curr_stock.select("sku", "sellable_units", "update_time", "update_date"))

# Join orders with placed time
vdc_ftr_ordered_time = vdc_ftr_orders_final.alias("a").join(placed_orders.alias("b"), col("a.orderjobid") == col("b.orderjobid"), "left") \
    .select("a.*", "b.ordered_time")

# Pre-Final Inventory Check (ordered_time > update_time)
pre_final_7 = vdc_ftr_ordered_time.alias("a").join(inv.alias("b"), 
    (col("a.sku_id") == col("b.sku")) & (col("a.ordered_time") > col("b.update_time")), "left"
).withColumn("rnk", row_number().over(Window.partitionBy("orderjobid", "sku_id", "combo_sku_id").orderBy(col("b.update_time").desc()))) \
.filter(col("rnk") == 1) \
.select(
    col("a.*"), col("b.update_time").alias("inv_ordered_time"), col("b.sellable_units").alias("order_time_qty")
)

# Final Inventory Check (vdc_ftr_time > update_time)
final_7_logic = pre_final_7.alias("a").join(inv.alias("b"),
    (col("a.sku_id") == col("b.sku")) & (col("a.vdc_ftr_time") > col("b.update_time")), "left"
).withColumn("rnk2", row_number().over(Window.partitionBy("orderjobid", "sku_id", "combo_sku_id").orderBy(col("b.update_time").desc()))) \
.filter(col("rnk2") == 1) \
.select(
    col("a.*"), col("b.update_time").alias("inv_vdc_ftr_time"), col("b.sellable_units").alias("vdc_ftr_time_qty")
)

# Final Selection for Insert
final_7_output = final_7_logic.alias("a") \
    .join(df_item_names.alias("b"), col("a.sku_id") == col("b.sku_id"), "left") \
    .join(stores_det.alias("c"), col("a.store_id") == col("c.store_id"), "left") \
    .select(
        to_date(col("a.ordered_time")).alias("order_dt"),
        col("a.store_id"),
        col("c.area"),
        col("c.city"),
        col("a.orderjobid"),
        col("a.sku_id"),
        col("a.combo_sku_id"),
        col("b.item_code"),
        col("b.item_name"),
        col("a.ordered_qty"),
        col("a.const_qty"),
        col("a.final_ordered_qty"),
        col("a.delivery_type"),
        col("a.reason"),
        col("a.vdc_ftr_flag"),
        col("a.ordered_time"),
        col("a.vdc_ftr_time"),
        col("a.inv_ordered_time"),
        col("a.order_time_qty"),
        col("a.inv_vdc_ftr_time"),
        col("a.vdc_ftr_time_qty")
    )

insert_into_target(spark, final_7_output, target_7)


# ==============================================================================
# BLOCK 8: Processing 65f98121a162a56a.efa1f375d76194fa.5dc47d46a08b42b7 (FTR ALERT)
# ==============================================================================

target_8 = "65f98121a162a56a.efa1f375d76194fa.5dc47d46a08b42b7"
spark.sql(f"DELETE FROM {target_8}")

# Tables
df_ftr_alert_source = spark.read.format("delta").table("6754af9632a2745e.a2064f061d17278b.026bebdcb2d91d13")
df_target_items = spark.read.format("delta").table("65f98121a162a56a.efa1f375d76194fa.a8eb7ae446365afd")
df_logs_8 = spark.read.format("delta").table("6754af9632a2745e.a2064f061d17278b.75460396915f2efe")
df_mim = spark.read.format("delta").table("65f98121a162a56a.efa1f375d76194fa.729cda63878716d8")
df_store_outlets = spark.read.format("delta").table("65f98121a162a56a.efa1f375d76194fa.c45b3b26ff0856f6")

# Valid Stores List
valid_stores = df_target_items.select("store_id").distinct()

# FTR Items CTE
ftr_items_8 = df_ftr_alert_source.join(valid_stores, df_ftr_alert_source.store_id.cast("double") == valid_stores.store_id, "inner") \
    .filter((col("dt") == current_date()) & (col("state") == 'PICKED')) \
    .select("dt", df_ftr_alert_source.store_id, "state", "orderjobid", "metadata")

# Old/New items logic (similar to Block 7)
old_8 = ftr_items_8.select(
    "dt", "store_id", "state", "orderjobid",
    explode(from_json(get_json_object(col("metadata"), "$.oldItems"), ArrayType(StringType()))).alias("value")
).select("orderjobid", get_json_object("value", "$.skuId").alias("sku_id"), get_json_object("value", "$.quantity").cast("int").alias("ordered_qty"))

new_8 = ftr_items_8.select(
    "dt", "store_id", "state", "orderjobid",
    explode(from_json(get_json_object(col("metadata"), "$.newItems"), ArrayType(StringType()))).alias("value")
).select("orderjobid", get_json_object("value", "$.skuId").alias("sku_id"), get_json_object("value", "$.quantity").cast("int").alias("new_qty"))

old_new_8 = old_8.alias("a").join(new_8.alias("b"), 
    (col("a.orderjobid") == col("b.orderjobid")) & (col("a.sku_id") == col("b.sku_id")), "left"
).select(
    "a.*", coalesce(col("b.new_qty"), lit(0)).alias("new_qty"), 
    when(col("ordered_qty") - coalesce(col("new_qty"), lit(0)) != 0, 1).otherwise(0).alias("ftr_flag")
)

# Order Logs CTE
order_logs_8 = df_logs_8.join(valid_stores, df_logs_8.store_id == valid_stores.store_id, "inner") \
    .filter((col("dt") == current_date()) & col("status").isin('PLACED', 'DELIVERY_DELIVERED')) \
    .withColumn("rnk", row_number().over(Window.partitionBy("job_id", "status").orderBy("update_time"))) \
    .filter(col("rnk") == 1) \
    .select("dt", df_logs_8.store_id, col("job_id").alias("orderjobid"), "status")

placed_8 = order_logs_8.filter(col("status") == 'PLACED').select("dt", "store_id", "orderjobid", "status").distinct()
del_8 = order_logs_8.filter(col("status") == 'DELIVERY_DELIVERED').select("orderjobid", "status").distinct()

del_orders2_8 = placed_8.alias("a").join(del_8.alias("b"), col("a.orderjobid") == col("b.orderjobid"), "inner") \
    .select("a.dt", "a.store_id", "a.orderjobid", "b.status", lit(1).alias("order_flag"))

ftr_orders_8 = old_new_8.filter(col("ftr_flag") == 1).select("dt", "orderjobid", "ftr_flag").distinct()

pre_final_8 = del_orders2_8.alias("a").join(ftr_orders_8.alias("b"), col("a.orderjobid") == col("b.orderjobid"), "left") \
    .select(
        month(to_date("a.dt")).alias("order_month"), 
        weekofyear(to_date("a.dt")).alias("order_week"), 
        "a.dt", "a.store_id", "a.orderjobid", "a.order_flag", "b.ftr_flag"
    )

mim_filtered = df_mim.filter(~col("store_id").isin('3141', '517194')).select(col("store_id").cast("string"), "date").distinct()

mim_split = pre_final_8.alias("a").join(mim_filtered.alias("b"), col("a.store_id") == col("b.store_id"), "left") \
    .withColumn("mim_store_identifier", when(col("a.dt") >= col("b.date"), 1).otherwise(0))

final_8 = mim_split.groupBy("dt", "store_id", "mim_store_identifier").agg(
    spark_sum("order_flag").alias("order_count"),
    coalesce(spark_sum("ftr_flag"), lit(0)).alias("ftr_count")
).withColumn("ftr_percent", 
    coalesce(round((col("order_count") - col("ftr_count")) * 100 / col("order_count"), 4), lit(0))
)

# Store Names
store_outlets = df_store_outlets.withColumn("rn", dense_rank().over(Window.partitionBy("store_id").orderBy(col("date").desc()))) \
    .filter(col("rn") == 1).select("store_id", "city", "outlet_name").distinct()

final_output_8 = final_8.alias("a").join(store_outlets.alias("b"), col("a.store_id") == col("b.store_id"), "left") \
    .select(
        col("a.store_id"), "ftr_count", "order_count", "ftr_percent",
        when(col("ftr_percent") < 99.3, 'alert').otherwise('no_alert').alias("alert_status"),
        col("b.city"), col("b.outlet_name").alias("store_name"), F.current_timestamp().alias("updated_time")
    )

insert_into_target(spark, final_output_8, target_8)


# ==============================================================================
# BLOCK 9: Processing 65f98121a162a56a.efa1f375d76194fa.fe9f903c54c06c6a (Timelegs)
# ==============================================================================

target_9 = "65f98121a162a56a.efa1f375d76194fa.fe9f903c54c06c6a"
spark.sql(f"DELETE FROM {target_9}")

# This block re-calculates the CTE from Block 4. 
# Since we already computed df_final_4 (Block 4 result), we can reuse it if it contains all necessary columns.
# Checking logic: Block 9 uses the exact same CTE structure as Block 4.
# Block 4 result DataFrame `df_final_4` (and `cte_calculated` inside it) matches the CTE used in Block 9.
# We just need to perform aggregation on `cte_calculated`.

# The filter conditions for Block 9 aggregation:
df_valid_stores_9 = spark.read.format("delta").table("65f98121a162a56a.efa1f375d76194fa.d916d45a9e673c6f") \
    .filter((col("dt") == date_sub(current_date(), 2)) & (F.lower(col("type")) == 'instacart')) \
    .select(col("store_id").cast("string")).distinct()

# Reuse `cte_calculated` from Block 4 section (it contains all fields needed like o2p, p2b etc)
# Note: Ensure `cte_calculated` is available in scope.
df_agg_9 = cte_calculated.join(df_valid_stores_9, cte_calculated.store_id == df_valid_stores_9.store_id, "inner") \
    .filter(
        (col("status") == 'DELIVERY_DELIVERED') & 
        (col("slotted") == 'FALSE') & 
        (col("dt") == current_date())
    ).groupBy("dt", "city", "store_id", "status", "slotted") \
    .agg(
        avg("o2p").alias("avg_o2c"),
        avg("p2b").alias("avg_p2b"),
        avg("c2p").alias("avg_c2p"),
        avg("o2b").alias("avg_o2b")
    )

# Apply Alert Logic
df_alerts_9 = df_agg_9.withColumn("alert_status", 
    when((col("avg_o2c") >= 0.5) | (col("avg_p2b") >= 0.5) | (col("avg_c2p") >= 1.8), 'alert').otherwise('no_alert')
).withColumn("reason",
    when(col("avg_o2c") >= 0.5, 'O2C')
    .when(col("avg_p2b") >= 0.5, 'P2B')
    .when(col("avg_c2p") >= 1.8, 'C2P')
).withColumn("last_updated_data_at", F.current_timestamp())

# Filter only alerts
df_final_alerts = df_alerts_9.filter(col("alert_status") == 'alert')

# Join with store names (reusing store_outlets from Block 8)
final_output_9 = df_final_alerts.alias("a").join(store_outlets.alias("b"), col("a.store_id") == col("b.store_id"), "left") \
    .select(
        col("a.last_updated_data_at"), col("a.dt"), col("a.city"), col("a.store_id"), col("a.status"), col("a.slotted"),
        col("a.avg_o2c"), col("a.avg_p2b"), col("a.avg_c2p"), col("a.alert_status"), col("a.reason"),
        col("b.outlet_name").alias("store_name"), col("a.avg_o2b")
    )

insert_into_target(spark, final_output_9, target_9)