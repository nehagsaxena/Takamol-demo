# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Checks
# MAGIC ## Takamol Demo - Nitaqat Compliance Advisor
# MAGIC 
# MAGIC This notebook implements comprehensive Data Quality checks:
# MAGIC - Completeness checks (null values)
# MAGIC - Validity checks (allowed values)
# MAGIC - Consistency checks (matching values)
# MAGIC - Uniqueness checks (duplicates)
# MAGIC - Anomaly detection (outliers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup & Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, current_date,
    count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    abs as spark_abs, round as spark_round, expr, concat, coalesce,
    countDistinct, isnan, isnull
)
from pyspark.sql.types import *

# Configuration
CATALOG = "takamol_demo"
BRONZE_SCHEMA = "bronze"
DQ_SCHEMA = "data_quality"

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Data Quality Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DQ_SCHEMA}")
print(f"Schema {CATALOG}.{DQ_SCHEMA} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Add Expectations (Constraints) to Bronze Tables
# MAGIC 
# MAGIC Databricks Expectations are declarative DQ rules enforced at the table level.

# COMMAND ----------

# Define expectations for raw_establishments
establishment_expectations = [
    ("est_id_not_null", "establishment_id IS NOT NULL"),
    ("est_name_not_null", "name IS NOT NULL"),
    ("est_valid_sector", "sector IN ('Retail', 'Construction', 'Information Technology', 'Healthcare', 'Hospitality', 'Manufacturing', 'Financial Services', 'Telecommunications')"),
    ("est_valid_size", "size_category IN ('Micro', 'Small', 'Medium', 'Large')"),
    ("est_valid_zone", "nitaqat_zone IN ('Platinum', 'High_Green', 'Mid_Green', 'Low_Green', 'Yellow', 'Red')"),
    ("est_employees_positive", "total_employees > 0"),
    ("est_saudi_not_negative", "saudi_employees >= 0"),
    ("est_employee_sum_matches", "total_employees = saudi_employees + non_saudi_employees"),
    ("est_saudization_rate_valid", "saudization_rate >= 0 AND saudization_rate <= 1")
]

# Apply expectations
print("Adding expectations to bronze.raw_establishments...")
for constraint_name, expression in establishment_expectations:
    try:
        spark.sql(f"""
            ALTER TABLE {BRONZE_SCHEMA}.raw_establishments 
            ADD CONSTRAINT {constraint_name} 
            EXPECT ({expression}) VIOLATION POLICY WARN
        """)
        print(f"  ✓ Added: {constraint_name}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  - Exists: {constraint_name}")
        else:
            print(f"  ✗ Error: {constraint_name} - {e}")

# COMMAND ----------

# Define expectations for raw_employees
employee_expectations = [
    ("emp_id_not_null", "employee_id IS NOT NULL"),
    ("emp_est_id_not_null", "establishment_id IS NOT NULL"),
    ("emp_nationality_not_null", "nationality IS NOT NULL"),
    ("emp_salary_positive", "salary_sar > 0"),
    ("emp_salary_reasonable", "salary_sar >= 1500 AND salary_sar <= 200000"),
    ("emp_hire_date_not_future", "hire_date <= current_date()"),
    ("emp_contract_type_valid", "contract_type IN ('Permanent', 'Fixed-term')"),
    ("emp_gender_valid", "gender IN ('Male', 'Female')")
]

print("\nAdding expectations to bronze.raw_employees...")
for constraint_name, expression in employee_expectations:
    try:
        spark.sql(f"""
            ALTER TABLE {BRONZE_SCHEMA}.raw_employees 
            ADD CONSTRAINT {constraint_name} 
            EXPECT ({expression}) VIOLATION POLICY WARN
        """)
        print(f"  ✓ Added: {constraint_name}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  - Exists: {constraint_name}")
        else:
            print(f"  ✗ Error: {constraint_name} - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DQ Rules Catalog
# MAGIC 
# MAGIC Document all DQ rules in a central catalog table.

# COMMAND ----------

# Define DQ rules catalog data
dq_rules_data = [
    # Completeness Rules
    ("DQ001", "Establishment ID Required", "Every establishment must have a unique ID", "bronze.raw_establishments", "establishment_id", "completeness", "establishment_id IS NOT NULL", "critical", True),
    ("DQ002", "Employee ID Required", "Every employee must have a unique ID", "bronze.raw_employees", "employee_id", "completeness", "employee_id IS NOT NULL", "critical", True),
    ("DQ003", "Establishment Name Required", "Every establishment must have a name", "bronze.raw_establishments", "name", "completeness", "name IS NOT NULL", "high", True),
    ("DQ004", "Nationality Required", "Every employee must have nationality recorded", "bronze.raw_employees", "nationality", "completeness", "nationality IS NOT NULL", "critical", True),
    
    # Validity Rules
    ("DQ005", "Valid Sector", "Sector must be from approved list", "bronze.raw_establishments", "sector", "validity", "sector IN (approved_list)", "high", True),
    ("DQ006", "Valid Nitaqat Zone", "Zone must be valid Nitaqat classification", "bronze.raw_establishments", "nitaqat_zone", "validity", "nitaqat_zone IN (valid_zones)", "critical", True),
    ("DQ007", "Valid Salary Range", "Salary must be within reasonable bounds (1500-200000 SAR)", "bronze.raw_employees", "salary_sar", "validity", "salary_sar BETWEEN 1500 AND 200000", "medium", True),
    ("DQ008", "Hire Date Not Future", "Employee hire date cannot be in the future", "bronze.raw_employees", "hire_date", "validity", "hire_date <= current_date()", "high", True),
    
    # Consistency Rules
    ("DQ009", "Employee Count Matches", "Total = Saudi + Non-Saudi employees", "bronze.raw_establishments", "total_employees", "consistency", "total_employees = saudi_employees + non_saudi_employees", "critical", True),
    ("DQ010", "Saudization Rate Consistent", "Rate must match calculated value", "bronze.raw_establishments", "saudization_rate", "consistency", "saudization_rate = saudi_employees / total_employees", "high", True),
    
    # Referential Integrity Rules
    ("DQ012", "Employee References Valid Establishment", "Every employee must belong to existing establishment", "bronze.raw_employees", "establishment_id", "consistency", "establishment_id EXISTS IN establishments", "critical", True),
    
    # Uniqueness Rules
    ("DQ013", "Establishment ID Unique", "No duplicate establishment IDs", "bronze.raw_establishments", "establishment_id", "uniqueness", "COUNT(DISTINCT establishment_id) = COUNT(*)", "critical", True),
    ("DQ014", "Employee ID Unique", "No duplicate employee IDs", "bronze.raw_employees", "employee_id", "uniqueness", "COUNT(DISTINCT employee_id) = COUNT(*)", "critical", True)
]

# Create schema for rules catalog
rules_schema = StructType([
    StructField("rule_id", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("rule_description", StringType(), True),
    StructField("target_table", StringType(), False),
    StructField("target_column", StringType(), True),
    StructField("rule_type", StringType(), False),
    StructField("rule_expression", StringType(), True),
    StructField("severity", StringType(), False),
    StructField("is_active", BooleanType(), False)
])

df_rules_catalog = spark.createDataFrame(dq_rules_data, rules_schema)
df_rules_catalog = (
    df_rules_catalog
    .withColumn("created_date", current_timestamp())
    .withColumn("last_updated", current_timestamp())
)

# Write to table
(
    df_rules_catalog
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{DQ_SCHEMA}.dq_rules_catalog")
)

print(f"✓ Created {DQ_SCHEMA}.dq_rules_catalog with {df_rules_catalog.count()} rules")
df_rules_catalog.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run DQ Validation Checks
# MAGIC 
# MAGIC Execute all DQ rules and store results.

# COMMAND ----------

# Read Bronze tables
df_establishments = spark.table(f"{BRONZE_SCHEMA}.raw_establishments")
df_employees = spark.table(f"{BRONZE_SCHEMA}.raw_employees")

est_count = df_establishments.count()
emp_count = df_employees.count()

print(f"Establishments to validate: {est_count:,}")
print(f"Employees to validate: {emp_count:,}")

# COMMAND ----------

# Run validation checks on Establishments
est_validations = df_establishments.agg(
    # Total
    count("*").alias("total_rows"),
    
    # Completeness
    spark_sum(when(col("establishment_id").isNull(), 1).otherwise(0)).alias("null_establishment_id"),
    spark_sum(when(col("name").isNull(), 1).otherwise(0)).alias("null_name"),
    spark_sum(when(col("sector").isNull(), 1).otherwise(0)).alias("null_sector"),
    
    # Validity
    spark_sum(when(~col("sector").isin('Retail', 'Construction', 'Information Technology', 'Healthcare', 'Hospitality', 'Manufacturing', 'Financial Services', 'Telecommunications'), 1).otherwise(0)).alias("invalid_sector"),
    spark_sum(when(~col("nitaqat_zone").isin('Platinum', 'High_Green', 'Mid_Green', 'Low_Green', 'Yellow', 'Red'), 1).otherwise(0)).alias("invalid_zone"),
    spark_sum(when(col("total_employees") <= 0, 1).otherwise(0)).alias("invalid_employee_count"),
    spark_sum(when((col("saudization_rate") < 0) | (col("saudization_rate") > 1), 1).otherwise(0)).alias("invalid_saudization_rate"),
    
    # Consistency
    spark_sum(when(col("total_employees") != (col("saudi_employees") + col("non_saudi_employees")), 1).otherwise(0)).alias("employee_sum_mismatch"),
    spark_sum(when(spark_abs(col("saudization_rate") - (col("saudi_employees") / col("total_employees"))) > 0.01, 1).otherwise(0)).alias("saudization_rate_mismatch"),
    
    # Uniqueness
    countDistinct("establishment_id").alias("distinct_ids")
).collect()[0]

# Run validation checks on Employees
emp_validations = df_employees.agg(
    count("*").alias("total_rows"),
    
    # Completeness
    spark_sum(when(col("employee_id").isNull(), 1).otherwise(0)).alias("null_employee_id"),
    spark_sum(when(col("establishment_id").isNull(), 1).otherwise(0)).alias("null_establishment_id"),
    spark_sum(when(col("nationality").isNull(), 1).otherwise(0)).alias("null_nationality"),
    spark_sum(when(col("job_title").isNull(), 1).otherwise(0)).alias("null_job_title"),
    
    # Validity
    spark_sum(when(col("salary_sar") <= 0, 1).otherwise(0)).alias("invalid_salary_zero"),
    spark_sum(when((col("salary_sar") < 1500) | (col("salary_sar") > 200000), 1).otherwise(0)).alias("salary_out_of_range"),
    spark_sum(when(col("hire_date") > current_date(), 1).otherwise(0)).alias("future_hire_date"),
    spark_sum(when(~col("contract_type").isin('Permanent', 'Fixed-term'), 1).otherwise(0)).alias("invalid_contract_type"),
    spark_sum(when(~col("gender").isin('Male', 'Female'), 1).otherwise(0)).alias("invalid_gender"),
    
    # Uniqueness
    countDistinct("employee_id").alias("distinct_ids")
).collect()[0]

# Check referential integrity
orphan_employees = df_employees.join(
    df_establishments.select("establishment_id"),
    on="establishment_id",
    how="left_anti"
).count()

print("Validation checks complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build DQ Validation Results Table

# COMMAND ----------

# Build validation results
validation_results = [
    # Establishments checks
    ("DQ001", "Establishment ID Required", "bronze.raw_establishments", "establishment_id", "completeness",
     est_count, est_count - est_validations["null_establishment_id"], est_validations["null_establishment_id"],
     round((est_count - est_validations["null_establishment_id"]) * 100.0 / est_count, 2)),
    
    ("DQ003", "Establishment Name Required", "bronze.raw_establishments", "name", "completeness",
     est_count, est_count - est_validations["null_name"], est_validations["null_name"],
     round((est_count - est_validations["null_name"]) * 100.0 / est_count, 2)),
    
    ("DQ005", "Valid Sector", "bronze.raw_establishments", "sector", "validity",
     est_count, est_count - est_validations["invalid_sector"], est_validations["invalid_sector"],
     round((est_count - est_validations["invalid_sector"]) * 100.0 / est_count, 2)),
    
    ("DQ006", "Valid Nitaqat Zone", "bronze.raw_establishments", "nitaqat_zone", "validity",
     est_count, est_count - est_validations["invalid_zone"], est_validations["invalid_zone"],
     round((est_count - est_validations["invalid_zone"]) * 100.0 / est_count, 2)),
    
    ("DQ009", "Employee Count Matches", "bronze.raw_establishments", "total_employees", "consistency",
     est_count, est_count - est_validations["employee_sum_mismatch"], est_validations["employee_sum_mismatch"],
     round((est_count - est_validations["employee_sum_mismatch"]) * 100.0 / est_count, 2)),
    
    ("DQ013", "Establishment ID Unique", "bronze.raw_establishments", "establishment_id", "uniqueness",
     est_count, est_validations["distinct_ids"], est_count - est_validations["distinct_ids"],
     round(est_validations["distinct_ids"] * 100.0 / est_count, 2)),
    
    # Employees checks
    ("DQ002", "Employee ID Required", "bronze.raw_employees", "employee_id", "completeness",
     emp_count, emp_count - emp_validations["null_employee_id"], emp_validations["null_employee_id"],
     round((emp_count - emp_validations["null_employee_id"]) * 100.0 / emp_count, 2)),
    
    ("DQ004", "Nationality Required", "bronze.raw_employees", "nationality", "completeness",
     emp_count, emp_count - emp_validations["null_nationality"], emp_validations["null_nationality"],
     round((emp_count - emp_validations["null_nationality"]) * 100.0 / emp_count, 2)),
    
    ("DQ007", "Valid Salary Range", "bronze.raw_employees", "salary_sar", "validity",
     emp_count, emp_count - emp_validations["salary_out_of_range"], emp_validations["salary_out_of_range"],
     round((emp_count - emp_validations["salary_out_of_range"]) * 100.0 / emp_count, 2)),
    
    ("DQ008", "Hire Date Not Future", "bronze.raw_employees", "hire_date", "validity",
     emp_count, emp_count - emp_validations["future_hire_date"], emp_validations["future_hire_date"],
     round((emp_count - emp_validations["future_hire_date"]) * 100.0 / emp_count, 2)),
    
    ("DQ012", "Employee References Valid Establishment", "bronze.raw_employees", "establishment_id", "consistency",
     emp_count, emp_count - orphan_employees, orphan_employees,
     round((emp_count - orphan_employees) * 100.0 / emp_count, 2)),
    
    ("DQ014", "Employee ID Unique", "bronze.raw_employees", "employee_id", "uniqueness",
     emp_count, emp_validations["distinct_ids"], emp_count - emp_validations["distinct_ids"],
     round(emp_validations["distinct_ids"] * 100.0 / emp_count, 2))
]

# Create schema
results_schema = StructType([
    StructField("rule_id", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("column_name", StringType(), True),
    StructField("check_type", StringType(), False),
    StructField("total_records", LongType(), False),
    StructField("passed_records", LongType(), False),
    StructField("failed_records", LongType(), False),
    StructField("pass_rate", DoubleType(), False)
])

df_validation_results = spark.createDataFrame(validation_results, results_schema)
df_validation_results = (
    df_validation_results
    .withColumn("status", when(col("failed_records") == 0, "PASSED").otherwise("FAILED"))
    .withColumn("check_timestamp", current_timestamp())
)

# Write to table
(
    df_validation_results
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{DQ_SCHEMA}.dq_validation_results")
)

print(f"✓ Created {DQ_SCHEMA}.dq_validation_results")
df_validation_results.orderBy("pass_rate").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. DQ Summary Metrics

# COMMAND ----------

df_results = spark.table(f"{DQ_SCHEMA}.dq_validation_results")

# Aggregate by table
df_summary = (
    df_results
    .groupBy("table_name")
    .agg(
        count("*").alias("total_rules_checked"),
        spark_sum(when(col("status") == "PASSED", 1).otherwise(0)).alias("rules_passed"),
        spark_sum(when(col("status") == "FAILED", 1).otherwise(0)).alias("rules_failed"),
        spark_sum("total_records").alias("total_records_checked"),
        spark_sum("passed_records").alias("total_records_passed"),
        spark_sum("failed_records").alias("total_records_failed"),
        spark_round(avg("pass_rate"), 2).alias("avg_record_pass_rate"),
        spark_min("pass_rate").alias("min_pass_rate"),
        spark_max("check_timestamp").alias("last_check_time")
    )
    .withColumn(
        "rule_pass_rate",
        spark_round(col("rules_passed") * 100.0 / col("total_rules_checked"), 1)
    )
    # Calculate weighted DQ score
    .withColumn("weighted_dq_score", col("avg_record_pass_rate"))  # Simplified
)

# Write to table
(
    df_summary
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{DQ_SCHEMA}.dq_summary_metrics")
)

print(f"✓ Created {DQ_SCHEMA}.dq_summary_metrics")
df_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Anomaly Detection

# COMMAND ----------

# Detect salary anomalies
df_salary_anomalies = (
    df_employees
    .filter((col("salary_sar") < 2000) | (col("salary_sar") > 100000))
    .select(
        lit("salary_anomaly").alias("anomaly_type"),
        col("employee_id").alias("record_id"),
        lit("bronze.raw_employees").alias("table_name"),
        lit("salary_sar").alias("column_name"),
        col("salary_sar").cast("string").alias("value"),
        concat(
            lit("Salary "),
            when(col("salary_sar") < 2000, lit("unusually low"))
            .otherwise(lit("unusually high")),
            lit(" for "),
            col("job_title")
        ).alias("anomaly_description"),
        when((col("salary_sar") < 1500) | (col("salary_sar") > 150000), lit("High"))
        .otherwise(lit("Medium")).alias("severity")
    )
)

# Detect saudization rate calculation mismatches
df_rate_anomalies = (
    df_establishments
    .filter(
        spark_abs(col("saudization_rate") - (col("saudi_employees") / col("total_employees"))) > 0.01
    )
    .select(
        lit("calculation_mismatch").alias("anomaly_type"),
        col("establishment_id").alias("record_id"),
        lit("bronze.raw_establishments").alias("table_name"),
        lit("saudization_rate").alias("column_name"),
        concat(
            col("saudization_rate").cast("string"),
            lit(" vs calculated: "),
            spark_round(col("saudi_employees") / col("total_employees"), 4).cast("string")
        ).alias("value"),
        lit("Stored saudization rate does not match calculated value").alias("anomaly_description"),
        lit("High").alias("severity")
    )
)

# Detect employee count outliers by size category
df_size_anomalies = (
    df_establishments
    .filter(
        ((col("size_category") == "Large") & (col("total_employees") < 100)) |
        ((col("size_category") == "Micro") & (col("total_employees") > 15)) |
        ((col("size_category") == "Small") & (col("total_employees") > 100))
    )
    .select(
        lit("employee_count_outlier").alias("anomaly_type"),
        col("establishment_id").alias("record_id"),
        lit("bronze.raw_establishments").alias("table_name"),
        lit("total_employees").alias("column_name"),
        col("total_employees").cast("string").alias("value"),
        concat(
            lit("Establishment has "),
            when(col("total_employees") < 10, lit("very few"))
            .otherwise(lit("unusually many")),
            lit(" employees for size category: "),
            col("size_category")
        ).alias("anomaly_description"),
        lit("Low").alias("severity")
    )
)

# Combine all anomalies
df_anomalies = (
    df_salary_anomalies
    .union(df_rate_anomalies)
    .union(df_size_anomalies)
    .withColumn("detected_at", current_timestamp())
)

# Write to table
(
    df_anomalies
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{DQ_SCHEMA}.dq_anomaly_detection")
)

print(f"✓ Created {DQ_SCHEMA}.dq_anomaly_detection with {df_anomalies.count()} anomalies")
df_anomalies.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create DQ Dashboard Views

# COMMAND ----------

# Overall DQ Dashboard View
spark.sql(f"""
CREATE OR REPLACE VIEW {DQ_SCHEMA}.vw_dq_dashboard AS
SELECT 
    'Overall Data Quality Score' AS metric_name,
    ROUND(AVG(weighted_dq_score), 1) AS metric_value,
    '%' AS metric_unit,
    CASE 
        WHEN AVG(weighted_dq_score) >= 95 THEN 'Excellent'
        WHEN AVG(weighted_dq_score) >= 85 THEN 'Good'
        WHEN AVG(weighted_dq_score) >= 70 THEN 'Fair'
        ELSE 'Poor'
    END AS status,
    MAX(last_check_time) AS as_of
FROM {DQ_SCHEMA}.dq_summary_metrics

UNION ALL

SELECT 
    'Total Records Validated',
    SUM(total_records_checked),
    'records',
    'Info',
    MAX(last_check_time)
FROM {DQ_SCHEMA}.dq_summary_metrics

UNION ALL

SELECT 
    'Failed Records',
    SUM(total_records_failed),
    'records',
    CASE WHEN SUM(total_records_failed) = 0 THEN 'Good' ELSE 'Action Needed' END,
    MAX(last_check_time)
FROM {DQ_SCHEMA}.dq_summary_metrics

UNION ALL

SELECT 
    'Anomalies Detected',
    COUNT(*),
    'issues',
    CASE WHEN COUNT(*) = 0 THEN 'Good' ELSE 'Review Needed' END,
    MAX(detected_at)
FROM {DQ_SCHEMA}.dq_anomaly_detection
""")

print(f"✓ Created {DQ_SCHEMA}.vw_dq_dashboard")

# COMMAND ----------

# DQ by Check Type View
spark.sql(f"""
CREATE OR REPLACE VIEW {DQ_SCHEMA}.vw_dq_by_check_type AS
SELECT 
    check_type,
    COUNT(*) AS rules_count,
    SUM(CASE WHEN status = 'PASSED' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
    ROUND(AVG(pass_rate), 1) AS avg_pass_rate
FROM {DQ_SCHEMA}.dq_validation_results
GROUP BY check_type
ORDER BY avg_pass_rate ASC
""")

print(f"✓ Created {DQ_SCHEMA}.vw_dq_by_check_type")

# COMMAND ----------

# Failed Rules Detail View
spark.sql(f"""
CREATE OR REPLACE VIEW {DQ_SCHEMA}.vw_dq_failed_rules AS
SELECT 
    rule_id,
    rule_name,
    table_name,
    column_name,
    check_type,
    failed_records,
    pass_rate,
    check_timestamp
FROM {DQ_SCHEMA}.dq_validation_results
WHERE status = 'FAILED'
ORDER BY failed_records DESC
""")

print(f"✓ Created {DQ_SCHEMA}.vw_dq_failed_rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Data Quality Layer Complete

# COMMAND ----------

# Final summary
print("=" * 60)
print("DATA QUALITY LAYER SUMMARY")
print("=" * 60)

dq_tables = [
    "dq_rules_catalog",
    "dq_validation_results",
    "dq_summary_metrics",
    "dq_anomaly_detection"
]

for table in dq_tables:
    count = spark.table(f"{DQ_SCHEMA}.{table}").count()
    print(f"  {DQ_SCHEMA}.{table}: {count:,} records")

print("\nViews created:")
print(f"  {DQ_SCHEMA}.vw_dq_dashboard")
print(f"  {DQ_SCHEMA}.vw_dq_by_check_type")
print(f"  {DQ_SCHEMA}.vw_dq_failed_rules")

print("=" * 60)
print("✓ Data Quality layer complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Demo Queries

# COMMAND ----------

# Show DQ Dashboard
print("DATA QUALITY DASHBOARD:")
spark.table(f"{DQ_SCHEMA}.vw_dq_dashboard").show(truncate=False)

# COMMAND ----------

# Show DQ by Check Type
print("DQ BY CHECK TYPE:")
spark.table(f"{DQ_SCHEMA}.vw_dq_by_check_type").show(truncate=False)

# COMMAND ----------

# Show Validation Results
print("ALL VALIDATION RESULTS:")
spark.table(f"{DQ_SCHEMA}.dq_validation_results").orderBy("pass_rate").show(truncate=False)

# COMMAND ----------

# Show Anomalies
print("ANOMALIES DETECTED:")
spark.table(f"{DQ_SCHEMA}.dq_anomaly_detection").show(truncate=False)
