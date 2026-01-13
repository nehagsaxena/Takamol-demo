# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Transformations
# MAGIC ## Takamol Demo - Nitaqat Compliance Advisor
# MAGIC 
# MAGIC This notebook creates Gold (business-ready) tables from Silver layer data.
# MAGIC 
# MAGIC **Tables Created:**
# MAGIC - KPI summaries and aggregations
# MAGIC - Executive dashboard metrics
# MAGIC - Reporting views

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup & Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, current_date, datediff,
    round as spark_round, ceil, concat, coalesce, 
    count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    countDistinct, first, collect_list, array_join, desc, asc,
    row_number, dense_rank, percent_rank
)
from pyspark.sql.window import Window

# Configuration
CATALOG = "takamol_demo"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Gold Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
print(f"Schema {CATALOG}.{GOLD_SCHEMA} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Gold: KPI Nitaqat Summary
# MAGIC 
# MAGIC **Source:** `silver.establishment_compliance_status`
# MAGIC 
# MAGIC **Purpose:** Zone-level aggregations for executive reporting

# COMMAND ----------

# Read Silver compliance status
df_compliance = spark.table(f"{SILVER_SCHEMA}.establishment_compliance_status")

# Aggregate by Nitaqat zone
df_nitaqat_summary = (
    df_compliance
    .groupBy("nitaqat_zone")
    .agg(
        count("*").alias("establishment_count"),
        spark_sum("total_employees").alias("total_workforce"),
        spark_sum("saudi_employees").alias("total_saudis"),
        spark_round(avg("saudization_pct"), 1).alias("avg_saudization_pct"),
        spark_round(spark_min("saudization_pct"), 1).alias("min_saudization_pct"),
        spark_round(spark_max("saudization_pct"), 1).alias("max_saudization_pct"),
        spark_sum("saudis_needed_for_next_zone").alias("total_saudis_needed_to_upgrade")
    )
)

# Calculate percentage of total
total_establishments = df_compliance.count()
df_nitaqat_summary = (
    df_nitaqat_summary
    .withColumn("pct_of_total", spark_round(col("establishment_count") * 100.0 / total_establishments, 1))
    .withColumn("calculated_at", current_timestamp())
    
    # Add zone order for sorting
    .withColumn(
        "zone_order",
        when(col("nitaqat_zone") == "Platinum", 1)
        .when(col("nitaqat_zone") == "High_Green", 2)
        .when(col("nitaqat_zone") == "Mid_Green", 3)
        .when(col("nitaqat_zone") == "Low_Green", 4)
        .when(col("nitaqat_zone") == "Yellow", 5)
        .otherwise(6)
    )
    .orderBy("zone_order")
    .drop("zone_order")
)

# Show results
print("KPI Nitaqat Summary:")
df_nitaqat_summary.show(truncate=False)

# COMMAND ----------

# Write to Gold table
(
    df_nitaqat_summary
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.kpi_nitaqat_summary")
)

print(f"✓ Written {df_nitaqat_summary.count()} records to {GOLD_SCHEMA}.kpi_nitaqat_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gold: KPI Sector Analysis
# MAGIC 
# MAGIC **Sources:** `silver.establishment_employee_summary` + `bronze.raw_sector_benchmarks`
# MAGIC 
# MAGIC **Purpose:** Industry-level comparison and benchmarking

# COMMAND ----------

# Read data
df_est_summary = spark.table(f"{SILVER_SCHEMA}.establishment_employee_summary")
df_benchmarks = spark.table(f"{BRONZE_SCHEMA}.raw_sector_benchmarks")

# Aggregate by sector
df_sector_agg = (
    df_est_summary
    .groupBy("sector")
    .agg(
        countDistinct("establishment_id").alias("establishment_count"),
        spark_sum("actual_employee_count").alias("total_employees"),
        spark_sum("actual_saudi_count").alias("total_saudi_employees"),
        spark_round(avg("saudization_pct"), 1).alias("avg_establishment_saudization_pct"),
        spark_round(avg("avg_salary"), 0).alias("avg_salary_sar"),
        spark_round(avg("avg_tenure_years"), 1).alias("avg_tenure_years"),
        
        # Zone distribution
        spark_sum(when(col("nitaqat_zone") == "Platinum", 1).otherwise(0)).alias("platinum_count"),
        spark_sum(when(col("nitaqat_zone").isin("High_Green", "Mid_Green", "Low_Green"), 1).otherwise(0)).alias("green_count"),
        spark_sum(when(col("nitaqat_zone") == "Yellow", 1).otherwise(0)).alias("yellow_count"),
        spark_sum(when(col("nitaqat_zone") == "Red", 1).otherwise(0)).alias("red_count")
    )
)

# Calculate overall saudization
df_sector_agg = df_sector_agg.withColumn(
    "overall_saudization_pct",
    spark_round(col("total_saudi_employees") * 100.0 / col("total_employees"), 1)
)

# JOIN with benchmarks
df_sector_analysis = (
    df_sector_agg
    .join(
        df_benchmarks.select(
            "sector",
            col("avg_saudization_pct").alias("benchmark_avg_pct"),
            col("median_saudization_pct").alias("benchmark_median_pct"),
            col("top_quartile_pct").alias("benchmark_top_quartile_pct"),
            col("trend_direction").alias("sector_trend")
        ),
        on="sector",
        how="left"
    )
    .withColumn("calculated_at", current_timestamp())
    .select(
        "sector",
        "establishment_count",
        "total_employees",
        "total_saudi_employees",
        "overall_saudization_pct",
        "avg_establishment_saudization_pct",
        "avg_salary_sar",
        "avg_tenure_years",
        "benchmark_avg_pct",
        "benchmark_median_pct",
        "benchmark_top_quartile_pct",
        "sector_trend",
        "platinum_count",
        "green_count",
        "yellow_count",
        "red_count",
        "calculated_at"
    )
)

# Show results
print("KPI Sector Analysis:")
df_sector_analysis.show(truncate=False)

# COMMAND ----------

# Write to Gold table
(
    df_sector_analysis
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.kpi_sector_analysis")
)

print(f"✓ Written {df_sector_analysis.count()} records to {GOLD_SCHEMA}.kpi_sector_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold: KPI Regional Analysis
# MAGIC 
# MAGIC **Source:** `silver.establishment_employee_summary`
# MAGIC 
# MAGIC **Purpose:** Geographic breakdown of compliance metrics

# COMMAND ----------

df_est_summary = spark.table(f"{SILVER_SCHEMA}.establishment_employee_summary")

df_regional_analysis = (
    df_est_summary
    .groupBy("region")
    .agg(
        countDistinct("establishment_id").alias("establishment_count"),
        spark_sum("actual_employee_count").alias("total_employees"),
        spark_sum("actual_saudi_count").alias("total_saudi_employees"),
        spark_round(avg("saudization_pct"), 1).alias("avg_establishment_saudization_pct"),
        spark_round(avg("avg_salary"), 0).alias("avg_salary_sar"),
        
        # Zone distribution
        spark_sum(when(col("nitaqat_zone") == "Platinum", 1).otherwise(0)).alias("platinum_count"),
        spark_sum(when(col("nitaqat_zone").isin("High_Green", "Mid_Green", "Low_Green"), 1).otherwise(0)).alias("green_count"),
        spark_sum(when(col("nitaqat_zone") == "Yellow", 1).otherwise(0)).alias("yellow_count"),
        spark_sum(when(col("nitaqat_zone") == "Red", 1).otherwise(0)).alias("red_count"),
        
        # Compliance rate
        spark_sum(when(col("compliance_status") == "Compliant", 1).otherwise(0)).alias("compliant_count")
    )
    # Calculate derived metrics
    .withColumn(
        "overall_saudization_pct",
        spark_round(col("total_saudi_employees") * 100.0 / col("total_employees"), 1)
    )
    .withColumn(
        "compliance_rate_pct",
        spark_round(col("compliant_count") * 100.0 / col("establishment_count"), 1)
    )
    .withColumn("calculated_at", current_timestamp())
    .drop("compliant_count")
    .orderBy(desc("establishment_count"))
)

# Show results
print("KPI Regional Analysis:")
df_regional_analysis.show(truncate=False)

# COMMAND ----------

# Write to Gold table
(
    df_regional_analysis
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.kpi_regional_analysis")
)

print(f"✓ Written {df_regional_analysis.count()} records to {GOLD_SCHEMA}.kpi_regional_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold: KPI Compliance Risk Score
# MAGIC 
# MAGIC **Sources:** `silver.establishment_compliance_status` + `silver.employee_contract_risk` + `bronze.raw_compliance_alerts`
# MAGIC 
# MAGIC **Purpose:** Composite risk scoring for each establishment
# MAGIC 
# MAGIC This is the most complex Gold table - combines multiple sources!

# COMMAND ----------

# Read all source tables
df_compliance = spark.table(f"{SILVER_SCHEMA}.establishment_compliance_status")
df_contract_risk = spark.table(f"{SILVER_SCHEMA}.employee_contract_risk")
df_alerts = spark.table(f"{BRONZE_SCHEMA}.raw_compliance_alerts")

# Aggregate contract risk by establishment
df_risk_agg = (
    df_contract_risk
    .groupBy("establishment_id")
    .agg(
        spark_sum(when(col("attrition_risk_level") == "Critical", 1).otherwise(0)).alias("critical_risk_employees"),
        spark_sum(when(col("attrition_risk_level") == "High", 1).otherwise(0)).alias("high_risk_employees"),
        spark_sum(when((col("is_saudi")) & (col("contract_status") == "Expiring Soon"), 1).otherwise(0)).alias("saudi_contracts_expiring_30d")
    )
)

# Aggregate alerts by establishment
df_alerts_agg = (
    df_alerts
    .filter(col("is_read") == False)
    .groupBy("establishment_id")
    .agg(
        count("*").alias("active_alerts"),
        spark_sum(when(col("severity") == "High", 1).otherwise(0)).alias("high_severity_alerts")
    )
)

# Join all together and calculate risk score
df_risk_score = (
    df_compliance
    .join(df_risk_agg, on="establishment_id", how="left")
    .join(df_alerts_agg, on="establishment_id", how="left")
    
    # Fill nulls with 0
    .fillna(0, subset=["critical_risk_employees", "high_risk_employees", "saudi_contracts_expiring_30d", "active_alerts", "high_severity_alerts"])
    
    # Calculate composite risk score (0-100, higher = more risk)
    .withColumn(
        "risk_score",
        spark_round(
            # Zone risk (0-40 points)
            when(col("nitaqat_zone") == "Red", 40)
            .when(col("nitaqat_zone") == "Yellow", 30)
            .when(col("nitaqat_zone") == "Low_Green", 20)
            .when(col("nitaqat_zone") == "Mid_Green", 10)
            .when(col("nitaqat_zone") == "High_Green", 5)
            .otherwise(0)
            +
            # Contract expiry risk (0-30 points)
            when(col("critical_risk_employees") * 10 > 30, 30)
            .otherwise(col("critical_risk_employees") * 10)
            +
            # Alert risk (0-20 points)
            when(col("high_severity_alerts") * 10 > 20, 20)
            .otherwise(col("high_severity_alerts") * 10)
            +
            # Gap risk (0-10 points)
            when(col("gap_to_low_green") > 0, 
                 when(col("gap_to_low_green") > 10, 10).otherwise(col("gap_to_low_green")))
            .otherwise(0)
        , 0)
    )
    
    # Add risk category
    .withColumn(
        "risk_category",
        when(col("nitaqat_zone") == "Red", "Critical")
        .when((col("nitaqat_zone") == "Yellow") | (col("critical_risk_employees") > 2), "High")
        .when((col("nitaqat_zone") == "Low_Green") | (col("high_severity_alerts") > 0), "Medium")
        .otherwise("Low")
    )
    
    .withColumn("calculated_at", current_timestamp())
    
    .select(
        "establishment_id",
        "name",
        "sector",
        "region",
        "nitaqat_zone",
        "saudization_pct",
        "saudis_needed_for_next_zone",
        "gap_to_low_green",
        "critical_risk_employees",
        "high_risk_employees",
        "saudi_contracts_expiring_30d",
        "active_alerts",
        "high_severity_alerts",
        "risk_score",
        "risk_category",
        "calculated_at"
    )
    .orderBy(desc("risk_score"))
)

# Show high risk establishments
print("KPI Compliance Risk Score (Top 10 by Risk):")
df_risk_score.show(10, truncate=False)

# COMMAND ----------

# Write to Gold table
(
    df_risk_score
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.kpi_compliance_risk_score")
)

print(f"✓ Written {df_risk_score.count()} records to {GOLD_SCHEMA}.kpi_compliance_risk_score")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold: Executive Dashboard Report
# MAGIC 
# MAGIC **Sources:** Multiple Gold tables
# MAGIC 
# MAGIC **Purpose:** Single-row summary for executive dashboards

# COMMAND ----------

# Read Gold tables
df_nitaqat = spark.table(f"{GOLD_SCHEMA}.kpi_nitaqat_summary")
df_risk = spark.table(f"{GOLD_SCHEMA}.kpi_compliance_risk_score")

# Calculate executive metrics
total_establishments = df_nitaqat.agg(spark_sum("establishment_count")).collect()[0][0]
total_workforce = df_nitaqat.agg(spark_sum("total_workforce")).collect()[0][0]
total_saudis = df_nitaqat.agg(spark_sum("total_saudis")).collect()[0][0]
overall_saudization = round(total_saudis * 100.0 / total_workforce, 1) if total_workforce > 0 else 0

# Zone counts
platinum_count = df_nitaqat.filter(col("nitaqat_zone") == "Platinum").select("establishment_count").collect()
platinum_count = platinum_count[0][0] if platinum_count else 0

green_count = df_nitaqat.filter(col("nitaqat_zone").contains("Green")).agg(spark_sum("establishment_count")).collect()[0][0] or 0

yellow_count = df_nitaqat.filter(col("nitaqat_zone") == "Yellow").select("establishment_count").collect()
yellow_count = yellow_count[0][0] if yellow_count else 0

red_count = df_nitaqat.filter(col("nitaqat_zone") == "Red").select("establishment_count").collect()
red_count = red_count[0][0] if red_count else 0

# Risk metrics
critical_risk = df_risk.filter(col("risk_category") == "Critical").count()
high_risk = df_risk.filter(col("risk_category") == "High").count()
avg_risk_score = df_risk.agg(spark_round(avg("risk_score"), 1)).collect()[0][0]

# Saudis needed
saudis_needed = df_nitaqat.filter(col("nitaqat_zone") != "Platinum").agg(spark_sum("total_saudis_needed_to_upgrade")).collect()[0][0] or 0

# Create executive dashboard dataframe
executive_data = [(
    "Overall",
    str(current_date()),
    int(total_establishments),
    int(total_workforce),
    int(total_saudis),
    float(overall_saudization),
    int(platinum_count),
    int(green_count),
    int(yellow_count),
    int(red_count),
    int(critical_risk),
    int(high_risk),
    float(avg_risk_score) if avg_risk_score else 0.0,
    int(saudis_needed)
)]

columns = [
    "report_level",
    "report_date", 
    "total_establishments",
    "total_workforce",
    "total_saudi_employees",
    "overall_saudization_pct",
    "platinum_establishments",
    "green_establishments",
    "yellow_establishments",
    "red_establishments",
    "critical_risk_count",
    "high_risk_count",
    "avg_risk_score",
    "total_saudis_needed_for_upgrades"
]

df_executive = spark.createDataFrame(executive_data, columns)
df_executive = df_executive.withColumn("generated_at", current_timestamp())

# Show results
print("Executive Dashboard Report:")
df_executive.show(truncate=False)

# COMMAND ----------

# Write to Gold table
(
    df_executive
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.report_executive_dashboard")
)

print(f"✓ Written executive dashboard to {GOLD_SCHEMA}.report_executive_dashboard")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gold Views for Reporting

# COMMAND ----------

# Create view: At-Risk Establishments
spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD_SCHEMA}.vw_at_risk_establishments AS
SELECT 
    establishment_id,
    name,
    sector,
    region,
    nitaqat_zone,
    saudization_pct,
    risk_score,
    risk_category,
    saudis_needed_for_next_zone,
    critical_risk_employees,
    active_alerts
FROM {GOLD_SCHEMA}.kpi_compliance_risk_score
WHERE risk_category IN ('Critical', 'High')
ORDER BY risk_score DESC
""")

print(f"✓ Created view {GOLD_SCHEMA}.vw_at_risk_establishments")

# COMMAND ----------

# Create view: Department Saudization
spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD_SCHEMA}.vw_department_saudization AS
SELECT 
    est.establishment_id,
    est.name AS establishment_name,
    est.sector,
    emp.department,
    COUNT(*) AS total_employees,
    SUM(CASE WHEN emp.is_saudi THEN 1 ELSE 0 END) AS saudi_employees,
    ROUND(SUM(CASE WHEN emp.is_saudi THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS department_saudization_pct,
    ROUND(AVG(emp.salary_sar), 0) AS avg_salary,
    ROUND(AVG(emp.tenure_years), 1) AS avg_tenure_years
FROM {SILVER_SCHEMA}.employees_cleaned emp
JOIN {SILVER_SCHEMA}.establishments_cleaned est 
    ON emp.establishment_id = est.establishment_id
GROUP BY 
    est.establishment_id, 
    est.name, 
    est.sector, 
    emp.department
ORDER BY 
    est.name, 
    department_saudization_pct ASC
""")

print(f"✓ Created view {GOLD_SCHEMA}.vw_department_saudization")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Gold Layer Complete

# COMMAND ----------

# Print summary of all Gold tables and views
print("=" * 60)
print("GOLD LAYER SUMMARY")
print("=" * 60)

gold_tables = [
    ("kpi_nitaqat_summary", "table"),
    ("kpi_sector_analysis", "table"),
    ("kpi_regional_analysis", "table"),
    ("kpi_compliance_risk_score", "table"),
    ("report_executive_dashboard", "table"),
    ("vw_at_risk_establishments", "view"),
    ("vw_department_saudization", "view")
]

for name, obj_type in gold_tables:
    try:
        if obj_type == "table":
            count = spark.table(f"{GOLD_SCHEMA}.{name}").count()
            print(f"  {GOLD_SCHEMA}.{name}: {count:,} records")
        else:
            count = spark.table(f"{GOLD_SCHEMA}.{name}").count()
            print(f"  {GOLD_SCHEMA}.{name} (view): {count:,} records")
    except Exception as e:
        print(f"  {GOLD_SCHEMA}.{name}: Error - {e}")

print("=" * 60)
print("✓ Gold layer transformations complete!")
print("✓ Data pipeline ready for AI/BI Genie and Dashboards")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Validation Queries

# COMMAND ----------

# Show executive summary
print("EXECUTIVE DASHBOARD:")
spark.table(f"{GOLD_SCHEMA}.report_executive_dashboard").show(truncate=False)

# COMMAND ----------

# Show top risk establishments
print("TOP 5 AT-RISK ESTABLISHMENTS:")
spark.table(f"{GOLD_SCHEMA}.vw_at_risk_establishments").limit(5).show(truncate=False)

# COMMAND ----------

# Show zone distribution
print("NITAQAT ZONE DISTRIBUTION:")
spark.table(f"{GOLD_SCHEMA}.kpi_nitaqat_summary").show(truncate=False)
