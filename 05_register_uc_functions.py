# Databricks notebook source
# MAGIC %md
# MAGIC # Mosaic AI Agent Tools - Unity Catalog Functions
# MAGIC ## Fixed Version with Case-Insensitive Matching
# MAGIC 
# MAGIC Run this notebook to register the functions in Unity Catalog for use in Playground.

# COMMAND ----------

# Configuration - UPDATE THESE IF NEEDED
CATALOG = "takamol_demo"
BRONZE_SCHEMA = "bronze"

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# Create schema for functions
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.nitaqat_tools")
print(f"✓ Schema {CATALOG}.nitaqat_tools ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 1: Get Establishment Status

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.nitaqat_tools.get_establishment_status(establishment_name STRING)
RETURNS TABLE (
    establishment_id STRING,
    name STRING,
    sector STRING,
    size_category STRING,
    region STRING,
    total_employees INT,
    saudi_employees INT,
    non_saudi_employees INT,
    saudization_pct DOUBLE,
    nitaqat_zone STRING,
    compliance_status STRING
)
RETURN
    SELECT 
        establishment_id,
        name,
        sector,
        size_category,
        region,
        total_employees,
        saudi_employees,
        non_saudi_employees,
        ROUND(saudization_rate * 100, 2) as saudization_pct,
        nitaqat_zone,
        CASE 
            WHEN nitaqat_zone IN ('Platinum', 'High_Green') THEN 'Compliant'
            WHEN nitaqat_zone IN ('Mid_Green', 'Low_Green') THEN 'At Risk'
            ELSE 'Non-Compliant'
        END as compliance_status
    FROM {BRONZE_SCHEMA}.raw_establishments
    WHERE LOWER(name) LIKE CONCAT('%', LOWER(establishment_name), '%')
    LIMIT 1
""")
print("✓ Registered: get_establishment_status")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 2: Get Establishments by Zone (FIXED)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.nitaqat_tools.get_establishments_by_zone(zone STRING)
RETURNS TABLE (
    name STRING,
    sector STRING,
    region STRING,
    total_employees INT,
    saudi_employees INT,
    saudization_pct DOUBLE,
    nitaqat_zone STRING
)
RETURN
    SELECT 
        name,
        sector,
        region,
        total_employees,
        saudi_employees,
        ROUND(saudization_rate * 100, 2) as saudization_pct,
        nitaqat_zone
    FROM {BRONZE_SCHEMA}.raw_establishments
    WHERE LOWER(nitaqat_zone) = LOWER(zone)
       OR LOWER(nitaqat_zone) = LOWER(REPLACE(zone, ' ', '_'))
       OR LOWER(REPLACE(nitaqat_zone, '_', ' ')) = LOWER(zone)
    ORDER BY saudization_rate ASC
""")
print("✓ Registered: get_establishments_by_zone")

# COMMAND ----------

# Test it
display(spark.sql(f"SELECT * FROM {CATALOG}.nitaqat_tools.get_establishments_by_zone('red')"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.nitaqat_tools.get_establishments_by_zone('Red')"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.nitaqat_tools.get_establishments_by_zone('high_green')"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.nitaqat_tools.get_establishments_by_zone('High Green')"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 3: Get Sector Summary

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.nitaqat_tools.get_sector_summary(sector_name STRING)
RETURNS TABLE (
    sector STRING,
    establishment_count BIGINT,
    total_employees BIGINT,
    total_saudis BIGINT,
    avg_saudization_pct DOUBLE,
    platinum_count BIGINT,
    green_count BIGINT,
    yellow_count BIGINT,
    red_count BIGINT
)
RETURN
    SELECT 
        sector,
        COUNT(*) as establishment_count,
        SUM(total_employees) as total_employees,
        SUM(saudi_employees) as total_saudis,
        ROUND(AVG(saudization_rate * 100), 2) as avg_saudization_pct,
        SUM(CASE WHEN nitaqat_zone = 'Platinum' THEN 1 ELSE 0 END) as platinum_count,
        SUM(CASE WHEN nitaqat_zone LIKE '%Green%' THEN 1 ELSE 0 END) as green_count,
        SUM(CASE WHEN nitaqat_zone = 'Yellow' THEN 1 ELSE 0 END) as yellow_count,
        SUM(CASE WHEN nitaqat_zone = 'Red' THEN 1 ELSE 0 END) as red_count
    FROM {BRONZE_SCHEMA}.raw_establishments
    WHERE LOWER(sector) = LOWER(sector_name)
       OR LOWER(sector) LIKE CONCAT('%', LOWER(sector_name), '%')
    GROUP BY sector
""")
print("✓ Registered: get_sector_summary")

# COMMAND ----------

# Test
display(spark.sql(f"SELECT * FROM {CATALOG}.nitaqat_tools.get_sector_summary('retail')"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 4: Get Saudi Employees with Expiring Contracts

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.nitaqat_tools.get_saudi_employees_expiring(establishment_name STRING, days_ahead INT)
RETURNS TABLE (
    employee_id STRING,
    job_title STRING,
    department STRING,
    salary_sar INT,
    contract_end_date DATE,
    days_until_expiry BIGINT
)
RETURN
    SELECT 
        e.employee_id,
        e.job_title,
        e.department,
        e.salary_sar,
        e.contract_end_date,
        DATEDIFF(e.contract_end_date, current_date()) as days_until_expiry
    FROM {BRONZE_SCHEMA}.raw_employees e
    JOIN {BRONZE_SCHEMA}.raw_establishments est ON e.establishment_id = est.establishment_id
    WHERE LOWER(est.name) LIKE CONCAT('%', LOWER(establishment_name), '%')
      AND e.nationality = 'Saudi'
      AND e.contract_end_date IS NOT NULL
      AND e.contract_end_date BETWEEN current_date() AND date_add(current_date(), days_ahead)
    ORDER BY e.contract_end_date ASC
""")
print("✓ Registered: get_saudi_employees_expiring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 5: Get Compliance Alerts

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.nitaqat_tools.get_compliance_alerts(establishment_name STRING)
RETURNS TABLE (
    establishment_name STRING,
    alert_type STRING,
    severity STRING,
    message STRING,
    recommended_action STRING,
    created_date TIMESTAMP
)
RETURN
    SELECT 
        establishment_name,
        alert_type,
        severity,
        message,
        recommended_action,
        created_date
    FROM {BRONZE_SCHEMA}.raw_compliance_alerts
    WHERE LOWER(establishment_name) LIKE CONCAT('%', LOWER(establishment_name), '%')
    ORDER BY 
        CASE severity WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END,
        created_date DESC
""")
print("✓ Registered: get_compliance_alerts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 6: Get Overall Summary

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.nitaqat_tools.get_overall_summary()
RETURNS TABLE (
    total_establishments BIGINT,
    total_workforce BIGINT,
    total_saudis BIGINT,
    overall_saudization_pct DOUBLE,
    platinum_count BIGINT,
    high_green_count BIGINT,
    mid_green_count BIGINT,
    low_green_count BIGINT,
    yellow_count BIGINT,
    red_count BIGINT,
    compliant_count BIGINT,
    non_compliant_count BIGINT
)
RETURN
    SELECT 
        COUNT(*) as total_establishments,
        SUM(total_employees) as total_workforce,
        SUM(saudi_employees) as total_saudis,
        ROUND(SUM(saudi_employees) * 100.0 / SUM(total_employees), 2) as overall_saudization_pct,
        SUM(CASE WHEN nitaqat_zone = 'Platinum' THEN 1 ELSE 0 END) as platinum_count,
        SUM(CASE WHEN nitaqat_zone = 'High_Green' THEN 1 ELSE 0 END) as high_green_count,
        SUM(CASE WHEN nitaqat_zone = 'Mid_Green' THEN 1 ELSE 0 END) as mid_green_count,
        SUM(CASE WHEN nitaqat_zone = 'Low_Green' THEN 1 ELSE 0 END) as low_green_count,
        SUM(CASE WHEN nitaqat_zone = 'Yellow' THEN 1 ELSE 0 END) as yellow_count,
        SUM(CASE WHEN nitaqat_zone = 'Red' THEN 1 ELSE 0 END) as red_count,
        SUM(CASE WHEN nitaqat_zone IN ('Platinum', 'High_Green', 'Mid_Green', 'Low_Green') THEN 1 ELSE 0 END) as compliant_count,
        SUM(CASE WHEN nitaqat_zone IN ('Yellow', 'Red') THEN 1 ELSE 0 END) as non_compliant_count
    FROM {BRONZE_SCHEMA}.raw_establishments
""")
print("✓ Registered: get_overall_summary")

# COMMAND ----------

# Test
display(spark.sql(f"SELECT * FROM {CATALOG}.nitaqat_tools.get_overall_summary()"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 7: Get Nitaqat Thresholds

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.nitaqat_tools.get_nitaqat_thresholds(sector_name STRING, size STRING)
RETURNS TABLE (
    sector STRING,
    size_category STRING,
    platinum_threshold_pct DOUBLE,
    high_green_threshold_pct DOUBLE,
    mid_green_threshold_pct DOUBLE,
    low_green_threshold_pct DOUBLE
)
RETURN
    SELECT 
        sector,
        size_category,
        platinum_threshold_pct,
        high_green_threshold_pct,
        mid_green_threshold_pct,
        low_green_threshold_pct
    FROM {BRONZE_SCHEMA}.raw_nitaqat_rules
    WHERE LOWER(sector) LIKE CONCAT('%', LOWER(sector_name), '%')
      AND LOWER(size_category) = LOWER(size)
""")
print("✓ Registered: get_nitaqat_thresholds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 8: Calculate Saudis Needed for Zone

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.nitaqat_tools.calculate_saudis_needed(establishment_name STRING, target_zone STRING)
RETURNS TABLE (
    name STRING,
    current_zone STRING,
    current_saudization_pct DOUBLE,
    current_saudi_employees INT,
    current_total_employees INT,
    target_zone STRING,
    target_threshold_pct DOUBLE,
    saudis_to_hire INT,
    projected_saudization_pct DOUBLE
)
RETURN
    WITH establishment AS (
        SELECT 
            e.name,
            e.sector,
            e.size_category,
            e.nitaqat_zone,
            ROUND(e.saudization_rate * 100, 2) as current_pct,
            e.saudi_employees,
            e.total_employees
        FROM {BRONZE_SCHEMA}.raw_establishments e
        WHERE LOWER(e.name) LIKE CONCAT('%', LOWER(establishment_name), '%')
        LIMIT 1
    ),
    thresholds AS (
        SELECT 
            r.platinum_threshold_pct,
            r.high_green_threshold_pct,
            r.mid_green_threshold_pct,
            r.low_green_threshold_pct
        FROM {BRONZE_SCHEMA}.raw_nitaqat_rules r
        JOIN establishment est ON r.sector = est.sector AND r.size_category = est.size_category
    ),
    target AS (
        SELECT 
            CASE 
                WHEN LOWER('{target_zone}') IN ('platinum') THEN t.platinum_threshold_pct
                WHEN LOWER('{target_zone}') IN ('high_green', 'high green', 'highgreen') THEN t.high_green_threshold_pct
                WHEN LOWER('{target_zone}') IN ('mid_green', 'mid green', 'midgreen') THEN t.mid_green_threshold_pct
                WHEN LOWER('{target_zone}') IN ('low_green', 'low green', 'lowgreen') THEN t.low_green_threshold_pct
                ELSE t.low_green_threshold_pct
            END as target_pct
        FROM thresholds t
    )
    SELECT 
        e.name,
        e.nitaqat_zone as current_zone,
        e.current_pct as current_saudization_pct,
        e.saudi_employees as current_saudi_employees,
        e.total_employees as current_total_employees,
        '{target_zone}' as target_zone,
        t.target_pct as target_threshold_pct,
        CASE 
            WHEN e.current_pct >= t.target_pct THEN 0
            ELSE CAST(CEIL((t.target_pct/100 * e.total_employees - e.saudi_employees) / (1 - t.target_pct/100)) AS INT)
        END as saudis_to_hire,
        CASE 
            WHEN e.current_pct >= t.target_pct THEN e.current_pct
            ELSE ROUND(
                (e.saudi_employees + CEIL((t.target_pct/100 * e.total_employees - e.saudi_employees) / (1 - t.target_pct/100))) * 100.0 / 
                (e.total_employees + CEIL((t.target_pct/100 * e.total_employees - e.saudi_employees) / (1 - t.target_pct/100)))
            , 2)
        END as projected_saudization_pct
    FROM establishment e
    CROSS JOIN target t
""")
print("✓ Registered: calculate_saudis_needed")

# COMMAND ----------

# Test
display(spark.sql(f"SELECT * FROM {CATALOG}.nitaqat_tools.calculate_saudis_needed('Al Faris', 'High_Green')"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 9: Simulate Workforce Change

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.nitaqat_tools.simulate_workforce_change(
    establishment_name STRING,
    saudi_hires INT,
    saudi_departures INT,
    non_saudi_hires INT,
    non_saudi_departures INT
)
RETURNS TABLE (
    name STRING,
    current_saudis INT,
    current_non_saudis INT,
    current_total INT,
    current_saudization_pct DOUBLE,
    current_zone STRING,
    new_saudis INT,
    new_non_saudis INT,
    new_total INT,
    new_saudization_pct DOUBLE,
    projected_zone STRING,
    zone_impact STRING
)
RETURN
    WITH current_state AS (
        SELECT 
            name,
            saudi_employees,
            non_saudi_employees,
            total_employees,
            ROUND(saudization_rate * 100, 2) as saudization_pct,
            nitaqat_zone,
            sector,
            size_category
        FROM {BRONZE_SCHEMA}.raw_establishments
        WHERE LOWER(name) LIKE CONCAT('%', LOWER(establishment_name), '%')
        LIMIT 1
    ),
    thresholds AS (
        SELECT r.*
        FROM {BRONZE_SCHEMA}.raw_nitaqat_rules r
        JOIN current_state c ON r.sector = c.sector AND r.size_category = c.size_category
    ),
    new_state AS (
        SELECT 
            c.*,
            GREATEST(0, c.saudi_employees + {saudi_hires} - {saudi_departures}) as new_saudis,
            GREATEST(0, c.non_saudi_employees + {non_saudi_hires} - {non_saudi_departures}) as new_non_saudis,
            GREATEST(0, c.saudi_employees + {saudi_hires} - {saudi_departures}) + 
            GREATEST(0, c.non_saudi_employees + {non_saudi_hires} - {non_saudi_departures}) as new_total
        FROM current_state c
    )
    SELECT 
        n.name,
        n.saudi_employees as current_saudis,
        n.non_saudi_employees as current_non_saudis,
        n.total_employees as current_total,
        n.saudization_pct as current_saudization_pct,
        n.nitaqat_zone as current_zone,
        n.new_saudis,
        n.new_non_saudis,
        n.new_total,
        ROUND(n.new_saudis * 100.0 / n.new_total, 2) as new_saudization_pct,
        CASE 
            WHEN (n.new_saudis * 100.0 / n.new_total) >= t.platinum_threshold_pct THEN 'Platinum'
            WHEN (n.new_saudis * 100.0 / n.new_total) >= t.high_green_threshold_pct THEN 'High_Green'
            WHEN (n.new_saudis * 100.0 / n.new_total) >= t.mid_green_threshold_pct THEN 'Mid_Green'
            WHEN (n.new_saudis * 100.0 / n.new_total) >= t.low_green_threshold_pct THEN 'Low_Green'
            WHEN (n.new_saudis * 100.0 / n.new_total) >= 6 THEN 'Yellow'
            ELSE 'Red'
        END as projected_zone,
        CASE 
            WHEN (n.new_saudis * 100.0 / n.new_total) > n.saudization_pct THEN 'IMPROVEMENT'
            WHEN (n.new_saudis * 100.0 / n.new_total) < n.saudization_pct THEN 'DECLINE'
            ELSE 'NO_CHANGE'
        END as zone_impact
    FROM new_state n
    CROSS JOIN thresholds t
""")
print("✓ Registered: simulate_workforce_change")

# COMMAND ----------

# Test
display(spark.sql(f"SELECT * FROM {CATALOG}.nitaqat_tools.simulate_workforce_change('Al Faris', 5, 0, 0, 3)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 10: Get All Employees for Establishment

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.nitaqat_tools.get_employees(establishment_name STRING)
RETURNS TABLE (
    employee_id STRING,
    nationality STRING,
    is_saudi STRING,
    job_title STRING,
    department STRING,
    salary_sar INT,
    contract_type STRING,
    hire_date DATE,
    contract_end_date DATE,
    gender STRING
)
RETURN
    SELECT 
        e.employee_id,
        e.nationality,
        CASE WHEN e.nationality = 'Saudi' THEN 'Yes' ELSE 'No' END as is_saudi,
        e.job_title,
        e.department,
        e.salary_sar,
        e.contract_type,
        e.hire_date,
        e.contract_end_date,
        e.gender
    FROM {BRONZE_SCHEMA}.raw_employees e
    JOIN {BRONZE_SCHEMA}.raw_establishments est ON e.establishment_id = est.establishment_id
    WHERE LOWER(est.name) LIKE CONCAT('%', LOWER(establishment_name), '%')
    ORDER BY e.nationality DESC, e.department, e.job_title
""")
print("✓ Registered: get_employees")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 11: Compare Multiple Establishments

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.nitaqat_tools.list_all_establishments()
RETURNS TABLE (
    name STRING,
    sector STRING,
    region STRING,
    size_category STRING,
    total_employees INT,
    saudi_employees INT,
    saudization_pct DOUBLE,
    nitaqat_zone STRING,
    compliance_status STRING
)
RETURN
    SELECT 
        name,
        sector,
        region,
        size_category,
        total_employees,
        saudi_employees,
        ROUND(saudization_rate * 100, 2) as saudization_pct,
        nitaqat_zone,
        CASE 
            WHEN nitaqat_zone IN ('Platinum', 'High_Green') THEN 'Compliant'
            WHEN nitaqat_zone IN ('Mid_Green', 'Low_Green') THEN 'At Risk'
            ELSE 'Non-Compliant'
        END as compliance_status
    FROM {BRONZE_SCHEMA}.raw_establishments
    ORDER BY saudization_rate DESC
""")
print("✓ Registered: list_all_establishments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: All Functions Registered

# COMMAND ----------

# List all registered functions
display(spark.sql(f"""
    SHOW FUNCTIONS IN {CATALOG}.nitaqat_tools
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Test All Functions

# COMMAND ----------

print("Testing all functions...\n")

print("1. get_establishment_status('Al Faris'):")
display(spark.sql(f"SELECT * FROM {CATALOG}.nitaqat_tools.get_establishment_status('Al Faris')"))

print("\n2. get_establishments_by_zone('Red'):")
display(spark.sql(f"SELECT * FROM {CATALOG}.nitaqat_tools.get_establishments_by_zone('Red')"))

print("\n3. get_sector_summary('Retail'):")
display(spark.sql(f"SELECT * FROM {CATALOG}.nitaqat_tools.get_sector_summary('Retail')"))

print("\n4. get_overall_summary():")
display(spark.sql(f"SELECT * FROM {CATALOG}.nitaqat_tools.get_overall_summary()"))

print("\n5. calculate_saudis_needed('Al Faris', 'High_Green'):")
display(spark.sql(f"SELECT * FROM {CATALOG}.nitaqat_tools.calculate_saudis_needed('Al Faris', 'High_Green')"))

print("\n6. simulate_workforce_change('Al Faris', 5, 0, 0, 3):")
display(spark.sql(f"SELECT * FROM {CATALOG}.nitaqat_tools.simulate_workforce_change('Al Faris', 5, 0, 0, 3)"))

print("\n✓ All functions working!")
