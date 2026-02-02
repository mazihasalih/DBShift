Here is the complete PySpark script for Databricks Serverless / Spark Connect, following all specified migration rules and constraints.

from pyspark.sql.functions import (
    col, lit, current_date, date_sub, coalesce, regexp_replace, 
    from_json, explode, get_json_object, array, when
)
from pyspark.sql.types import ArrayType, StringType
from delta.tables import DeltaTable

# Step 1: Preparation - Variable Definition
# SQL: set start_date=current_date-2; set end_date=current_date;
# We use Spark functions for date logic directly in filters to use Serverless time.
start_date_expr = date_sub(current_date(), 2)
end_date_expr = current_date()

# Define Table Names
# Preserving database.schema.table structure as per instructions
source_table_name = "66d9c488b5fa766b.3ad2e1180e2ad695.e91f525043751268"
target_table_name = "65f98121a162a56a.efa1f375d76194fa.f6071cb0f02112c9"

# Step 2: Identifying Tables and Creating Base DataFrame
# CTE: base
# Logic: distinct select and date filtering
df_base = spark.read.format("delta").table(source_table_name).alias("base") \
    .filter(col("updated_at").between(start_date_expr, end_date_expr)) \
    .select(
        col("order_id"),
        col("pg_chargeback"),
        col("PG_CHARGEBACK_REVERSAL"),
        col("PG_REFUND"),
        col("PG_TRANSACTION"),
        col("CREATED_AT"),
        col("TRANSACTION_ID"),
        col("lob"),
        col("updated_at")
    ).distinct()

# Step 3: Performing Transformations
# The SQL performs the same extraction logic on 4 different JSON columns.
# We define a function to handle the 'lateral flatten' and extraction logic to keep code clean.

def process_json_category(df_input, json_col_name, description_val):
    """
    Simulates: FROM base, lateral flatten(input => parse_json(json_col_name)) vm
    And extracts fields with cleaning logic.
    """
    
    # Parse JSON string to Array of Strings, then Explode (Lateral Flatten)
    # Snowflake 'flatten' on a list produces rows. 'explode' does the equivalent in Spark.
    df_exploded = df_input.withColumn(
        "json_arr", 
        from_json(col(json_col_name), ArrayType(StringType()))
    ).withColumn(
        "vm_value", 
        explode(col("json_arr")) # explode (inner) removes nulls/empty arrays, similar to lateral join
    )

    # Apply transformations and cleaning logic
    # Snowflake: regexp_replace(val, '\\(|\\)', '') -> Spark: regexp_replace(val, '[\\(\\)]', '')
    # Snowflake: coalesce(val, '') -> Spark: coalesce(val, lit(''))
    
    df_transformed = df_exploded.select(
        col("order_id"),
        lit(description_val).alias("description"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.pg"), "[\\(\\)]", ""), lit("")).alias("PG"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.merchantId"), "[\\(\\)]", ""), lit("")).alias("MID"),
        coalesce(col("TRANSACTION_ID"), lit("")).alias("txn_id"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.pgTransactionId"), "[\\(\\)]", ""), lit("")).alias("Pg_TXN_ID"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.transactionType"), "[\\(\\)]", ""), lit("")).alias("TXN_TYPE"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.transactionTime"), "[\\(\\)]", ""), lit("")).alias("TXN_DATE"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.settlementDate"), "[\\(\\)]", ""), lit("")).alias("SETTLEMENT_DATE"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.pgTransactionAmount"), "[\\(\\)]", ""), lit("")).alias("GROSS_TXN_VALUE"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.pgCommission"), "[\\(\\)]", ""), lit("")).alias("PG_COMMISSION"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.cgst"), "[\\(\\)]", ""), lit("")).alias("CGST"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.sgst"), "[\\(\\)]", ""), lit("")).alias("SGST"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.igst"), "[\\(\\)]", ""), lit("")).alias("IGST"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.amountTransferredToNodal"), "[\\(\\)]", ""), lit("")).alias("AMOUNT_TRANSFERRED_TO_NODAL"),
        
        # UTR Logic: replace(replace(REPLACE(vm.value:utr, ']',''),'[',''),'"','')
        # Spark Regex equivalent: remove [, ], or "
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.utr"), "[\\[\\]\"]", ""), lit("")).alias("UTR"),
        
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.pgRefundId"), "[\\(\\)]", ""), lit("")).alias("REFUND_ID"),
        coalesce(col("CREATED_AT"), lit("")).alias("CREATED_AT"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.paymentMode"), "[\\(\\)]", ""), lit("")).alias("paymentMode"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.bank"), "[\\(\\)]", ""), lit("")).alias("cardIssuer"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.cardType"), "[\\(\\)]", ""), lit("")).alias("cardType"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.paymentMode"), "[\\(\\)]", ""), lit("")).alias("createdBy"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.cardNumber"), "[\\(\\)]", ""), lit("")).alias("cardNumber"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.binNumber"), "[\\(\\)]", ""), lit("")).alias("binNumber"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.last4Digits"), "[\\(\\)]", ""), lit("")).alias("cardLastFourDigits"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.authCode"), "[\\(\\)]", ""), lit("")).alias("authCode"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.bankReference"), "[\\(\\)]", ""), lit("")).alias("bankReference"),
        coalesce(regexp_replace(get_json_object(col("vm_value"), "$.rrn"), "[\\(\\)]", ""), lit("")).alias("rrn"),
        coalesce(col("lob"), lit("")).alias("lob"),
        coalesce(col("updated_at"), lit("")).alias("updated_at"),
        col("vm_value").alias("arr") # Keeping the raw extracted JSON object string
    )
    
    return df_transformed

# Generate DataFrames for each CTE section
df_pg_chargeback = process_json_category(df_base, "pg_chargeback", "PG_CHARGEBACK")
df_pg_reversal = process_json_category(df_base, "PG_CHARGEBACK_REVERSAL", "PG_CHARGEBACK_REVERSAL")
df_pg_refund = process_json_category(df_base, "PG_REFUND", "PG_REFUND")
df_pg_transaction = process_json_category(df_base, "PG_TRANSACTION", "PG_TRANSACTION")

# Union all parts (UNION implies distinct in SQL, but usually UNION ALL is more efficient. 
# The SQL uses UNION, so we use unionByName followed by distinct if strict SQL adherence is required, 
# or just distinct() at the end. The SQL provided has `select * from ... union select * ...` which creates distinct set)
df_union = df_pg_chargeback.unionByName(df_pg_reversal) \
    .unionByName(df_pg_refund) \
    .unionByName(df_pg_transaction) \
    .distinct()

# Prepare Source DataFrame (alias 'b' in SQL)
df_source = df_union.alias("b")

# Step 4: Applying Filters / Constraints for Merge
# Target Table (alias 'a' in SQL)
target_delta_table = DeltaTable.forName(spark, target_table_name)

# Define Merge Condition
# a.PG=b.pg and a.MID=b.mid ...
merge_condition = (
    (col("a.PG") == col("b.PG")) &
    (col("a.MID") == col("b.MID")) &
    (col("a.Pg_TXN_ID") == col("b.Pg_TXN_ID")) &
    (col("a.TXN_TYPE") == col("b.TXN_TYPE")) &
    (col("a.AMOUNT_TRANSFERRED_TO_NODAL") == col("b.AMOUNT_TRANSFERRED_TO_NODAL")) &
    (col("a.UTR") == col("b.UTR")) &
    (col("a.REFUND_ID") == col("b.REFUND_ID")) &
    (col("a.paymentMode") == col("b.paymentMode"))
)

# Step 6: Finalizing the Output - Merge Execution
# WHEN NOT MATCHED THEN INSERT
# We map the source columns to the target columns explicitly as defined in the SQL INSERT block.

target_delta_table.alias("a").merge(
    df_source,
    merge_condition
).whenNotMatchedInsert(
    values={
        "order_id": col("b.order_id"),
        "description": col("b.description"),
        "PG": col("b.PG"),
        "MID": col("b.MID"),
        "txn_id": col("b.txn_id"),
        "Pg_TXN_ID": col("b.Pg_TXN_ID"),
        "TXN_TYPE": col("b.TXN_TYPE"),
        "TXN_DATE": col("b.TXN_DATE"),
        "SETTLEMENT_DATE": col("b.SETTLEMENT_DATE"),
        "GROSS_TXN_VALUE": col("b.GROSS_TXN_VALUE"),
        "PG_COMMISSION": col("b.PG_COMMISSION"),
        "CGST": col("b.CGST"),
        "SGST": col("b.SGST"),
        "IGST": col("b.IGST"),
        "AMOUNT_TRANSFERRED_TO_NODAL": col("b.AMOUNT_TRANSFERRED_TO_NODAL"),
        "UTR": col("b.UTR"),
        "REFUND_ID": col("b.REFUND_ID"),
        "CREATED_AT": col("b.CREATED_AT"),
        "paymentMode": col("b.paymentMode"),
        "cardIssuer": col("b.cardIssuer"),
        "cardType": col("b.cardType"),
        "createdBy": col("b.createdBy"),
        "cardNumber": col("b.cardNumber"),
        "binNumber": col("b.binNumber"),
        "cardLastFourDigits": col("b.cardLastFourDigits"),
        "authCode": col("b.authCode"),
        "bankReference": col("b.bankReference"),
        "rrn": col("b.rrn"),
        "lob": col("b.lob"),
        "updated_at": col("b.updated_at"),
        "arr": col("b.arr")
    }
).execute()