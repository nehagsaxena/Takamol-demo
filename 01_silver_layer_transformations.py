# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Transformations
# MAGIC ## Takamol Demo - Nitaqat Compliance Advisor
# MAGIC 
# MAGIC This notebook transforms Bronze (raw) data into Silver (cleaned & enriched) tables.
# MAGIC 
# MAGIC **Transformations:**
# MAGIC - Data cleaning and standardization
# MAGIC - Derived field calculations
# MAGIC - Table joins and enrichment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup & Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, current_date, datediff, year, 
    round as spark_round, ceil, concat, coalesce, trim, upper, lower,
    count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    countDistinct, expr
)
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, BooleanType

# Configuration
CATALOG = "takamol_demo"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
print(f"Schema {CATALOG}.{SILVER_SCHEMA} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Silver: Establishments Cleaned
# MAGIC 
# MAGIC **Source:** `bronze.raw_establishments`
# MAGIC 
# MAGIC **Transformations:**
# MAGIC - Convert saudization_rate to percentage
# MAGIC - Add compliance_status derived field
# MAGIC - Add days_since_registration
# MAGIC - Add employee_count_bucket for reporting

# COMMAND ----------

# Read Bronze establishments
df_establishments_bronze = spark.table(f"{BRONZE_SCHEMA}.raw_establishments")

print(f"Bronze establishments count: {df_establishments_bronze.count()}")
df_establishments_bronze.printSchema()

# COMMAND ----------

# Transform to Silver
df_establishments_silver = (
    df_establishments_bronze
    # Filter out nulls on key fields
    .filter(col("establishment_id").isNotNull())
    
    # Clean and standardize
    .withColumn("name", trim(col("name")))
    .withColumn("sector", trim(col("sector")))
    .withColumn("region", trim(col("region")))
    
    # Convert saudization rate to percentage
    .withColumn("saudization_pct", spark_round(col("saudization_rate") * 100, 2))
    
    # Add compliance status based on Nitaqat zone
    .withColumn(
        "compliance_status",
        when(col("nitaqat_zone").isin("Platinum", "High_Green"), "Compliant")
        .when(col("nitaqat_zone").isin("Mid_Green", "Low_Green"), "At Risk")
        .otherwise("Non-Compliant")
    )
    
    # Add days since registration
    .withColumn("days_since_registration", datediff(current_date(), col("registration_date")))
    
    # Add registration year for time-based analysis
    .withColumn("registration_year", year(col("registration_date")))
    
    # Add employee count bucket for reporting
    .withColumn(
        "employee_count_bucket",
        when(col("total_employees") < 10, "1-9")
        .when(col("total_employees") < 50, "10-49")
        .when(col("total_employees") < 200, "50-199")
        .when(col("total_employees") < 500, "200-499")
        .otherwise("500+")
    )
    
    # Add processing metadata
    .withColumn("_silver_processed_at", current_timestamp())
    
    # Select final columns
    .select(
        "establishment_id",
        "name",
        "commercial_registration",
        "sector",
        "size_category",
        "region",
        "total_employees",
        "saudi_employees",
        "non_saudi_employees",
        "saudization_pct",
        "nitaqat_zone",
        "compliance_status",
        "registration_date",
        "registration_year",
        "days_since_registration",
        "employee_count_bucket",
        "last_updated",
        "_silver_processed_at"
    )
)

# Show sample
print("Silver Establishments Sample:")
df_establishments_silver.show(5, truncate=False)

# COMMAND ----------

# Write to Silver table
(
    df_establishments_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.establishments_cleaned")
)

print(f"✓ Written {df_establishments_silver.count()} records to {SILVER_SCHEMA}.establishments_cleaned")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver: Employees Cleaned
# MAGIC 
# MAGIC **Source:** `bronze.raw_employees`
# MAGIC 
# MAGIC **Transformations:**
# MAGIC - Add is_saudi boolean flag
# MAGIC - Calculate tenure in days and years
# MAGIC - Add contract_status (Active, Expiring Soon, Expired)
# MAGIC - Add salary_band classification

# COMMAND ----------

# Read Bronze employees
df_employees_bronze = spark.table(f"{BRONZE_SCHEMA}.raw_employees")

print(f"Bronze employees count: {df_employees_bronze.count()}")

# COMMAND ----------

# Transform to Silver
df_employees_silver = (
    df_employees_bronze
    # Filter out nulls on key fields
    .filter(col("employee_id").isNotNull())
    .filter(col("establishment_id").isNotNull())
    
    # Clean and standardize
    .withColumn("nationality", trim(col("nationality")))
    .withColumn("job_title", trim(col("job_title")))
    .withColumn("department", trim(col("department")))
    
    # Add is_saudi boolean flag (critical for Saudization calculations)
    .withColumn("is_saudi", when(col("nationality") == "Saudi", True).otherwise(False))
    
    # Calculate tenure
    .withColumn("tenure_days", datediff(current_date(), col("hire_date")))
    .withColumn("tenure_years", spark_round(col("tenure_days") / 365.25, 1))
    
    # Add contract status
    .withColumn(
        "contract_status",
        when(col("contract_end_date").isNull(), "No End Date")
        .when(col("contract_end_date") < current_date(), "Expired")
        .when(col("contract_end_date") < expr("date_add(current_date(), 30)"), "Expiring Soon")
        .when(col("contract_end_date") < expr("date_add(current_date(), 90)"), "Expiring in 90 Days")
        .otherwise("Active")
    )
    
    # Add salary band classification
    .withColumn(
        "salary_band",
        when(col("salary_sar") < 5000, "Entry Level")
        .when(col("salary_sar") < 10000, "Mid Level")
        .when(col("salary_sar") < 20000, "Senior Level")
        .otherwise("Executive")
    )
    
    # Add processing metadata
    .withColumn("_silver_processed_at", current_timestamp())
    
    # Select final columns
    .select(
        "employee_id",
        "establishment_id",
        "nationality",
        "is_saudi",
        "job_title",
        "department",
        "salary_sar",
        "salary_band",
        "contract_type",
        "hire_date",
        "contract_end_date",
        "contract_status",
        "tenure_days",
        "tenure_years",
        "is_remote",
        "gender",
        "_silver_processed_at"
    )
)

# Show sample
print("Silver Employees Sample:")
df_employees_silver.show(5, truncate=False)

# COMMAND ----------

# Write to Silver table
(
    df_employees_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.employees_cleaned")
)

print(f"✓ Written {df_employees_silver.count()} records to {SILVER_SCHEMA}.employees_cleaned")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver: Establishment Employee Summary
# MAGIC 
# MAGIC **Sources:** `silver.establishments_cleaned` + `silver.employees_cleaned`
# MAGIC 
# MAGIC **Transformations:**
# MAGIC - JOIN establishments with aggregated employee metrics
# MAGIC - Calculate actual counts, averages, and risk indicators

# COMMAND ----------

# Read Silver tables
df_est = spark.table(f"{SILVER_SCHEMA}.establishments_cleaned")
df_emp = spark.table(f"{SILVER_SCHEMA}.employees_cleaned")

# Aggregate employee metrics per establishment
df_emp_agg = (
    df_emp
    .groupBy("establishment_id")
    .agg(
        count("*").alias("actual_employee_count"),
        spark_sum(when(col("is_saudi"), 1).otherwise(0)).alias("actual_saudi_count"),
        spark_sum(when(~col("is_saudi"), 1).otherwise(0)).alias("actual_non_saudi_count"),
        spark_round(avg("salary_sar"), 0).alias("avg_salary"),
        spark_round(avg(when(col("is_saudi"), col("salary_sar"))), 0).alias("avg_saudi_salary"),
        spark_round(avg(when(~col("is_saudi"), col("salary_sar"))), 0).alias("avg_non_saudi_salary"),
        spark_round(avg("tenure_years"), 1).alias("avg_tenure_years"),
        countDistinct("department").alias("department_count"),
        countDistinct("job_title").alias("job_title_count"),
        spark_sum(when((col("contract_status") == "Expiring Soon") & col("is_saudi"), 1).otherwise(0)).alias("saudi_contracts_expiring_soon"),
        spark_sum(when((col("contract_status") == "Expiring in 90 Days") & col("is_saudi"), 1).otherwise(0)).alias("saudi_contracts_expiring_90d")
    )
)

# JOIN with establishments
df_est_emp_summary = (
    df_est
    .join(df_emp_agg, on="establishment_id", how="left")
    .withColumn("_silver_processed_at", current_timestamp())
    .select(
        "establishment_id",
        "name",
        "sector",
        "size_category",
        "region",
        "nitaqat_zone",
        "compliance_status",
        "saudization_pct",
        "actual_employee_count",
        "actual_saudi_count",
        "actual_non_saudi_count",
        "avg_salary",
        "avg_saudi_salary",
        "avg_non_saudi_salary",
        "avg_tenure_years",
        "department_count",
        "job_title_count",
        "saudi_contracts_expiring_soon",
        "saudi_contracts_expiring_90d",
        "_silver_processed_at"
    )
)

# Show sample
print("Establishment Employee Summary Sample:")
df_est_emp_summary.show(5, truncate=False)

# COMMAND ----------

# Write to Silver table
(
    df_est_emp_summary
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.establishment_employee_summary")
)

print(f"✓ Written {df_est_emp_summary.count()} records to {SILVER_SCHEMA}.establishment_employee_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver: Establishment Compliance Status
# MAGIC 
# MAGIC **Sources:** `silver.establishments_cleaned` + `bronze.raw_nitaqat_rules`
# MAGIC 
# MAGIC **Transformations:**
# MAGIC - JOIN with Nitaqat rules to get thresholds
# MAGIC - Calculate gap to each zone
# MAGIC - Calculate Saudis needed for next zone upgrade

# COMMAND ----------

# Read Nitaqat rules from Bronze
df_rules = spark.table(f"{BRONZE_SCHEMA}.raw_nitaqat_rules")
df_est = spark.table(f"{SILVER_SCHEMA}.establishments_cleaned")

# JOIN establishments with rules
df_compliance = (
    df_est
    .join(
        df_rules.select(
            "sector", 
            "size_category",
            "platinum_threshold_pct",
            "high_green_threshold_pct",
            "mid_green_threshold_pct",
            "low_green_threshold_pct"
        ),
        on=["sector", "size_category"],
        how="left"
    )
    # Calculate gaps to each zone
    .withColumn("gap_to_platinum", spark_round(col("platinum_threshold_pct") - col("saudization_pct"), 2))
    .withColumn("gap_to_high_green", spark_round(col("high_green_threshold_pct") - col("saudization_pct"), 2))
    .withColumn("gap_to_mid_green", spark_round(col("mid_green_threshold_pct") - col("saudization_pct"), 2))
    .withColumn("gap_to_low_green", spark_round(col("low_green_threshold_pct") - col("saudization_pct"), 2))
    
    # Calculate Saudis needed for next zone
    # Formula: ceil((target_pct/100 * total - current_saudi) / (1 - target_pct/100))
    .withColumn(
        "saudis_needed_for_next_zone",
        when(col("nitaqat_zone") == "Platinum", 0)
        .when(col("nitaqat_zone") == "High_Green", 
              ceil((col("platinum_threshold_pct")/100 * col("total_employees") - col("saudi_employees")) / (1 - col("platinum_threshold_pct")/100)))
        .when(col("nitaqat_zone") == "Mid_Green",
              ceil((col("high_green_threshold_pct")/100 * col("total_employees") - col("saudi_employees")) / (1 - col("high_green_threshold_pct")/100)))
        .when(col("nitaqat_zone") == "Low_Green",
              ceil((col("mid_green_threshold_pct")/100 * col("total_employees") - col("saudi_employees")) / (1 - col("mid_green_threshold_pct")/100)))
        .when(col("nitaqat_zone") == "Yellow",
              ceil((col("low_green_threshold_pct")/100 * col("total_employees") - col("saudi_employees")) / (1 - col("low_green_threshold_pct")/100)))
        .otherwise(
              ceil((col("low_green_threshold_pct")/100 * col("total_employees") - col("saudi_employees")) / (1 - col("low_green_threshold_pct")/100)))
    )
    
    # Add next target zone
    .withColumn(
        "next_target_zone",
        when(col("nitaqat_zone") == "Platinum", "Already at Platinum")
        .when(col("nitaqat_zone") == "High_Green", "Platinum")
        .when(col("nitaqat_zone") == "Mid_Green", "High_Green")
        .when(col("nitaqat_zone") == "Low_Green", "Mid_Green")
        .when(col("nitaqat_zone") == "Yellow", "Low_Green")
        .otherwise("Low_Green")
    )
    
    .withColumn("_silver_processed_at", current_timestamp())
    
    .select(
        "establishment_id",
        "name",
        "sector",
        "size_category",
        "region",
        "total_employees",
        "saudi_employees",
        "saudization_pct",
        "nitaqat_zone",
        "compliance_status",
        "platinum_threshold_pct",
        "high_green_threshold_pct",
        "mid_green_threshold_pct",
        "low_green_threshold_pct",
        "gap_to_platinum",
        "gap_to_high_green",
        "gap_to_mid_green",
        "gap_to_low_green",
        "saudis_needed_for_next_zone",
        "next_target_zone",
        "_silver_processed_at"
    )
)

# Show sample
print("Establishment Compliance Status Sample:")
df_compliance.show(5, truncate=False)

# COMMAND ----------

# Write to Silver table
(
    df_compliance
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.establishment_compliance_status")
)

print(f"✓ Written {df_compliance.count()} records to {SILVER_SCHEMA}.establishment_compliance_status")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver: Employee Contract Risk
# MAGIC 
# MAGIC **Sources:** `silver.employees_cleaned` + `silver.establishments_cleaned`
# MAGIC 
# MAGIC **Transformations:**
# MAGIC - JOIN employees with establishment details
# MAGIC - Calculate attrition risk level
# MAGIC - Calculate impact on Saudization if employee leaves

# COMMAND ----------

df_emp = spark.table(f"{SILVER_SCHEMA}.employees_cleaned")
df_est = spark.table(f"{SILVER_SCHEMA}.establishments_cleaned")

# JOIN and calculate risk
df_contract_risk = (
    df_emp
    .join(
        df_est.select(
            col("establishment_id"),
            col("name").alias("establishment_name"),
            col("sector"),
            col("nitaqat_zone"),
            col("saudization_pct"),
            col("saudi_employees"),
            col("total_employees")
        ),
        on="establishment_id",
        how="inner"
    )
    # Calculate attrition risk level
    .withColumn(
        "attrition_risk_level",
        when((col("is_saudi")) & (col("contract_status").isin("Expiring Soon", "Expired")), "Critical")
        .when((col("is_saudi")) & (col("contract_status") == "Expiring in 90 Days"), "High")
        .when((col("is_saudi")) & (col("nitaqat_zone").isin("Yellow", "Red", "Low_Green")), "Medium")
        .otherwise("Low")
    )
    # Calculate Saudization impact if this employee leaves
    .withColumn(
        "saudization_pct_if_leaves",
        when(col("is_saudi"),
             spark_round((col("saudi_employees") - 1) / col("total_employees") * 100, 2))
        .otherwise(col("saudization_pct"))
    )
    
    .withColumn("_silver_processed_at", current_timestamp())
    
    .select(
        "employee_id",
        "establishment_id",
        "establishment_name",
        "sector",
        "nitaqat_zone",
        "nationality",
        "is_saudi",
        "job_title",
        "department",
        "salary_sar",
        "salary_band",
        "hire_date",
        "contract_end_date",
        "contract_status",
        "tenure_years",
        "attrition_risk_level",
        "saudization_pct_if_leaves",
        "_silver_processed_at"
    )
)

# Show sample - focus on high risk
print("Employee Contract Risk Sample (High Risk):")
df_contract_risk.filter(col("attrition_risk_level").isin("Critical", "High")).show(10, truncate=False)

# COMMAND ----------

# Write to Silver table
(
    df_contract_risk
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.employee_contract_risk")
)

print(f"✓ Written {df_contract_risk.count()} records to {SILVER_SCHEMA}.employee_contract_risk")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Silver Layer Complete

# COMMAND ----------

# Print summary of all Silver tables
print("=" * 60)
print("SILVER LAYER SUMMARY")
print("=" * 60)

silver_tables = [
    "establishments_cleaned",
    "employees_cleaned", 
    "establishment_employee_summary",
    "establishment_compliance_status",
    "employee_contract_risk"
]

for table in silver_tables:
    count = spark.table(f"{SILVER_SCHEMA}.{table}").count()
    print(f"  {SILVER_SCHEMA}.{table}: {count:,} records")

print("=" * 60)
print("✓ Silver layer transformations complete!")
print("✓ Proceed to Gold layer notebook for KPI aggregations")
