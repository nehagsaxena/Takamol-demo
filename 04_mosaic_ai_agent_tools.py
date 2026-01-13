# Databricks notebook source
# MAGIC %md
# MAGIC # Mosaic AI Agent Tools - Bronze Layer
# MAGIC ## Nitaqat Compliance Advisor - For Playground Demo
# MAGIC 
# MAGIC These functions work directly on Bronze layer data and can be registered as tools
# MAGIC for use in Databricks Playground to demonstrate Agentic AI capabilities.

# COMMAND ----------

# Configuration
CATALOG = "takamol_demo"
BRONZE_SCHEMA = "bronze"

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool Functions

# COMMAND ----------

from typing import Dict, List, Any, Optional
import json
from datetime import datetime, timedelta

# COMMAND ----------

def get_establishment_status(establishment_name: str) -> Dict[str, Any]:
    """
    Get the current Nitaqat compliance status for an establishment.
    
    Args:
        establishment_name: Name or partial name of the establishment
        
    Returns:
        Establishment details including zone, saudization rate, and compliance status
    """
    
    df = spark.sql(f"""
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
            registration_date
        FROM {BRONZE_SCHEMA}.raw_establishments
        WHERE LOWER(name) LIKE LOWER('%{establishment_name}%')
        LIMIT 1
    """)
    
    if df.count() == 0:
        return {"error": f"No establishment found matching '{establishment_name}'"}
    
    row = df.collect()[0]
    
    # Determine compliance status
    zone = row["nitaqat_zone"]
    if zone in ["Platinum", "High_Green"]:
        compliance_status = "Compliant"
    elif zone in ["Mid_Green", "Low_Green"]:
        compliance_status = "At Risk"
    else:
        compliance_status = "Non-Compliant"
    
    return {
        "establishment_id": row["establishment_id"],
        "name": row["name"],
        "sector": row["sector"],
        "size_category": row["size_category"],
        "region": row["region"],
        "total_employees": int(row["total_employees"]),
        "saudi_employees": int(row["saudi_employees"]),
        "non_saudi_employees": int(row["non_saudi_employees"]),
        "saudization_pct": float(row["saudization_pct"]),
        "nitaqat_zone": zone,
        "compliance_status": compliance_status
    }

# COMMAND ----------

def get_nitaqat_thresholds(sector: str, size_category: str) -> Dict[str, Any]:
    """
    Get Nitaqat zone thresholds for a specific sector and size combination.
    
    Args:
        sector: Industry sector (e.g., Retail, Construction, Healthcare)
        size_category: Size category (Micro, Small, Medium, Large)
        
    Returns:
        Threshold percentages for each Nitaqat zone
    """
    
    df = spark.sql(f"""
        SELECT 
            sector,
            size_category,
            platinum_threshold_pct,
            high_green_threshold_pct,
            mid_green_threshold_pct,
            low_green_threshold_pct
        FROM {BRONZE_SCHEMA}.raw_nitaqat_rules
        WHERE sector = '{sector}' AND size_category = '{size_category}'
        LIMIT 1
    """)
    
    if df.count() == 0:
        return {"error": f"No thresholds found for {sector} / {size_category}"}
    
    row = df.collect()[0]
    
    return {
        "sector": row["sector"],
        "size_category": row["size_category"],
        "thresholds": {
            "platinum": float(row["platinum_threshold_pct"]),
            "high_green": float(row["high_green_threshold_pct"]),
            "mid_green": float(row["mid_green_threshold_pct"]),
            "low_green": float(row["low_green_threshold_pct"])
        }
    }

# COMMAND ----------

def calculate_saudis_needed_for_zone(
    establishment_name: str, 
    target_zone: str
) -> Dict[str, Any]:
    """
    Calculate how many Saudi employees need to be hired to reach a target Nitaqat zone.
    
    Args:
        establishment_name: Name of the establishment
        target_zone: Target zone (Platinum, High_Green, Mid_Green, Low_Green)
        
    Returns:
        Current status, target requirements, and number of Saudis to hire
    """
    
    # Get establishment status
    status = get_establishment_status(establishment_name)
    if "error" in status:
        return status
    
    # Get thresholds for this sector/size
    thresholds = get_nitaqat_thresholds(status["sector"], status["size_category"])
    if "error" in thresholds:
        return thresholds
    
    # Map target zone to threshold
    zone_map = {
        "Platinum": thresholds["thresholds"]["platinum"],
        "High_Green": thresholds["thresholds"]["high_green"],
        "Mid_Green": thresholds["thresholds"]["mid_green"],
        "Low_Green": thresholds["thresholds"]["low_green"]
    }
    
    if target_zone not in zone_map:
        return {"error": f"Invalid target zone. Choose from: Platinum, High_Green, Mid_Green, Low_Green"}
    
    target_pct = zone_map[target_zone]
    current_saudis = status["saudi_employees"]
    current_total = status["total_employees"]
    current_pct = status["saudization_pct"]
    
    # Already at target?
    if current_pct >= target_pct:
        return {
            "establishment": status["name"],
            "current_zone": status["nitaqat_zone"],
            "current_saudization_pct": current_pct,
            "target_zone": target_zone,
            "target_threshold_pct": target_pct,
            "saudis_to_hire": 0,
            "message": f"Already at or above {target_zone} threshold!"
        }
    
    # Calculate: (target_pct/100 * (total + X) = saudis + X) => solve for X
    target_decimal = target_pct / 100
    saudis_needed = int((target_decimal * current_total - current_saudis) / (1 - target_decimal)) + 1
    
    # Project new values
    new_total = current_total + saudis_needed
    new_saudis = current_saudis + saudis_needed
    new_pct = round((new_saudis / new_total) * 100, 2)
    
    return {
        "establishment": status["name"],
        "current_zone": status["nitaqat_zone"],
        "current_saudization_pct": current_pct,
        "current_saudi_employees": current_saudis,
        "current_total_employees": current_total,
        "target_zone": target_zone,
        "target_threshold_pct": target_pct,
        "saudis_to_hire": saudis_needed,
        "projected_saudization_pct": new_pct,
        "projected_total_employees": new_total
    }

# COMMAND ----------

def get_employees_by_establishment(
    establishment_name: str,
    saudi_only: bool = False,
    expiring_contracts_days: int = None
) -> Dict[str, Any]:
    """
    Get employee details for an establishment.
    
    Args:
        establishment_name: Name of the establishment
        saudi_only: If True, only return Saudi employees
        expiring_contracts_days: If provided, only return employees with contracts expiring within this many days
        
    Returns:
        List of employees with their details
    """
    
    # First get establishment ID
    est_df = spark.sql(f"""
        SELECT establishment_id, name 
        FROM {BRONZE_SCHEMA}.raw_establishments
        WHERE LOWER(name) LIKE LOWER('%{establishment_name}%')
        LIMIT 1
    """)
    
    if est_df.count() == 0:
        return {"error": f"No establishment found matching '{establishment_name}'"}
    
    est_row = est_df.collect()[0]
    est_id = est_row["establishment_id"]
    est_name = est_row["name"]
    
    # Build query conditions
    conditions = [f"establishment_id = '{est_id}'"]
    
    if saudi_only:
        conditions.append("nationality = 'Saudi'")
    
    if expiring_contracts_days:
        conditions.append(f"contract_end_date <= date_add(current_date(), {expiring_contracts_days})")
        conditions.append("contract_end_date >= current_date()")
    
    where_clause = " AND ".join(conditions)
    
    df = spark.sql(f"""
        SELECT 
            employee_id,
            nationality,
            job_title,
            department,
            salary_sar,
            contract_type,
            hire_date,
            contract_end_date,
            gender
        FROM {BRONZE_SCHEMA}.raw_employees
        WHERE {where_clause}
        ORDER BY contract_end_date ASC
    """)
    
    employees = []
    for row in df.collect():
        emp = {
            "employee_id": row["employee_id"],
            "nationality": row["nationality"],
            "job_title": row["job_title"],
            "department": row["department"],
            "salary_sar": int(row["salary_sar"]),
            "contract_type": row["contract_type"],
            "hire_date": str(row["hire_date"]),
            "gender": row["gender"]
        }
        if row["contract_end_date"]:
            emp["contract_end_date"] = str(row["contract_end_date"])
        employees.append(emp)
    
    return {
        "establishment_name": est_name,
        "establishment_id": est_id,
        "filters_applied": {
            "saudi_only": saudi_only,
            "expiring_within_days": expiring_contracts_days
        },
        "employee_count": len(employees),
        "employees": employees
    }

# COMMAND ----------

def simulate_workforce_change(
    establishment_name: str,
    saudi_hires: int = 0,
    saudi_departures: int = 0,
    non_saudi_hires: int = 0,
    non_saudi_departures: int = 0
) -> Dict[str, Any]:
    """
    Simulate the impact of workforce changes on Nitaqat zone.
    
    Args:
        establishment_name: Name of the establishment
        saudi_hires: Number of Saudis to hire
        saudi_departures: Number of Saudis leaving
        non_saudi_hires: Number of non-Saudis to hire
        non_saudi_departures: Number of non-Saudis leaving
        
    Returns:
        Before and after comparison showing zone impact
    """
    
    status = get_establishment_status(establishment_name)
    if "error" in status:
        return status
    
    thresholds = get_nitaqat_thresholds(status["sector"], status["size_category"])
    if "error" in thresholds:
        return thresholds
    
    # Current state
    current_saudis = status["saudi_employees"]
    current_non_saudis = status["non_saudi_employees"]
    current_total = status["total_employees"]
    current_pct = status["saudization_pct"]
    current_zone = status["nitaqat_zone"]
    
    # New state
    new_saudis = max(0, current_saudis + saudi_hires - saudi_departures)
    new_non_saudis = max(0, current_non_saudis + non_saudi_hires - non_saudi_departures)
    new_total = new_saudis + new_non_saudis
    new_pct = round((new_saudis / new_total) * 100, 2) if new_total > 0 else 0
    
    # Determine new zone
    t = thresholds["thresholds"]
    if new_pct >= t["platinum"]:
        new_zone = "Platinum"
    elif new_pct >= t["high_green"]:
        new_zone = "High_Green"
    elif new_pct >= t["mid_green"]:
        new_zone = "Mid_Green"
    elif new_pct >= t["low_green"]:
        new_zone = "Low_Green"
    elif new_pct >= 6:
        new_zone = "Yellow"
    else:
        new_zone = "Red"
    
    # Zone change analysis
    zone_order = ["Red", "Yellow", "Low_Green", "Mid_Green", "High_Green", "Platinum"]
    current_idx = zone_order.index(current_zone) if current_zone in zone_order else -1
    new_idx = zone_order.index(new_zone) if new_zone in zone_order else -1
    
    if new_idx > current_idx:
        impact = "UPGRADE"
    elif new_idx < current_idx:
        impact = "DOWNGRADE"
    else:
        impact = "NO_CHANGE"
    
    return {
        "establishment": status["name"],
        "scenario": {
            "saudi_hires": saudi_hires,
            "saudi_departures": saudi_departures,
            "non_saudi_hires": non_saudi_hires,
            "non_saudi_departures": non_saudi_departures
        },
        "before": {
            "saudi_employees": current_saudis,
            "non_saudi_employees": current_non_saudis,
            "total_employees": current_total,
            "saudization_pct": current_pct,
            "nitaqat_zone": current_zone
        },
        "after": {
            "saudi_employees": new_saudis,
            "non_saudi_employees": new_non_saudis,
            "total_employees": new_total,
            "saudization_pct": new_pct,
            "nitaqat_zone": new_zone
        },
        "zone_impact": impact,
        "pct_change": round(new_pct - current_pct, 2)
    }

# COMMAND ----------

def generate_hiring_plan(
    establishment_name: str,
    target_zone: str,
    monthly_hiring_capacity: int = 3,
    avg_monthly_salary: int = 8000
) -> Dict[str, Any]:
    """
    Generate a hiring plan with timeline and budget to reach target zone.
    
    Args:
        establishment_name: Name of the establishment
        target_zone: Target Nitaqat zone
        monthly_hiring_capacity: Max Saudis that can be hired per month
        avg_monthly_salary: Average monthly salary for new hires (SAR)
        
    Returns:
        Month-by-month hiring plan with costs
    """
    
    calc = calculate_saudis_needed_for_zone(establishment_name, target_zone)
    if "error" in calc:
        return calc
    
    saudis_needed = calc["saudis_to_hire"]
    
    if saudis_needed == 0:
        return {
            "establishment": calc["establishment"],
            "message": f"Already at {target_zone}. No hiring needed.",
            "current_zone": calc["current_zone"]
        }
    
    # Build month-by-month plan
    months_needed = (saudis_needed + monthly_hiring_capacity - 1) // monthly_hiring_capacity
    
    monthly_plan = []
    remaining = saudis_needed
    cumulative_hires = 0
    cumulative_monthly_cost = 0
    
    current_saudis = calc["current_saudi_employees"]
    current_total = calc["current_total_employees"]
    
    for month in range(1, months_needed + 1):
        hires = min(remaining, monthly_hiring_capacity)
        remaining -= hires
        cumulative_hires += hires
        
        new_saudis = current_saudis + cumulative_hires
        new_total = current_total + cumulative_hires
        new_pct = round((new_saudis / new_total) * 100, 2)
        
        cumulative_monthly_cost += hires * avg_monthly_salary
        
        monthly_plan.append({
            "month": month,
            "hires_this_month": hires,
            "cumulative_hires": cumulative_hires,
            "projected_saudization_pct": new_pct,
            "new_monthly_payroll_sar": hires * avg_monthly_salary,
            "cumulative_monthly_payroll_sar": cumulative_monthly_cost
        })
    
    return {
        "establishment": calc["establishment"],
        "current_zone": calc["current_zone"],
        "target_zone": target_zone,
        "total_saudis_to_hire": saudis_needed,
        "months_to_complete": months_needed,
        "monthly_hiring_rate": monthly_hiring_capacity,
        "avg_salary_sar": avg_monthly_salary,
        "total_annual_cost_sar": saudis_needed * avg_monthly_salary * 12,
        "monthly_plan": monthly_plan
    }

# COMMAND ----------

def get_establishments_by_zone(nitaqat_zone: str) -> Dict[str, Any]:
    """
    Get all establishments in a specific Nitaqat zone.
    
    Args:
        nitaqat_zone: Zone to filter (Platinum, High_Green, Mid_Green, Low_Green, Yellow, Red)
        
    Returns:
        List of establishments in that zone with key metrics
    """
    
    df = spark.sql(f"""
        SELECT 
            establishment_id,
            name,
            sector,
            region,
            total_employees,
            saudi_employees,
            ROUND(saudization_rate * 100, 2) as saudization_pct,
            nitaqat_zone
        FROM {BRONZE_SCHEMA}.raw_establishments
        WHERE nitaqat_zone = '{nitaqat_zone}'
        ORDER BY saudization_rate ASC
    """)
    
    establishments = []
    for row in df.collect():
        establishments.append({
            "name": row["name"],
            "sector": row["sector"],
            "region": row["region"],
            "total_employees": int(row["total_employees"]),
            "saudi_employees": int(row["saudi_employees"]),
            "saudization_pct": float(row["saudization_pct"])
        })
    
    return {
        "zone": nitaqat_zone,
        "count": len(establishments),
        "establishments": establishments
    }

# COMMAND ----------

def get_sector_summary(sector: str) -> Dict[str, Any]:
    """
    Get summary statistics for a specific sector.
    
    Args:
        sector: Industry sector name
        
    Returns:
        Sector-level aggregations and zone distribution
    """
    
    df = spark.sql(f"""
        SELECT 
            COUNT(*) as establishment_count,
            SUM(total_employees) as total_employees,
            SUM(saudi_employees) as total_saudis,
            ROUND(AVG(saudization_rate * 100), 2) as avg_saudization_pct,
            SUM(CASE WHEN nitaqat_zone = 'Platinum' THEN 1 ELSE 0 END) as platinum_count,
            SUM(CASE WHEN nitaqat_zone LIKE '%Green%' THEN 1 ELSE 0 END) as green_count,
            SUM(CASE WHEN nitaqat_zone = 'Yellow' THEN 1 ELSE 0 END) as yellow_count,
            SUM(CASE WHEN nitaqat_zone = 'Red' THEN 1 ELSE 0 END) as red_count
        FROM {BRONZE_SCHEMA}.raw_establishments
        WHERE sector = '{sector}'
    """)
    
    if df.count() == 0:
        return {"error": f"Sector '{sector}' not found"}
    
    row = df.collect()[0]
    
    # Get benchmark
    bench_df = spark.sql(f"""
        SELECT avg_saudization_pct, median_saudization_pct, trend_direction
        FROM {BRONZE_SCHEMA}.raw_sector_benchmarks
        WHERE sector = '{sector}'
    """)
    
    benchmark = {}
    if bench_df.count() > 0:
        b_row = bench_df.collect()[0]
        benchmark = {
            "industry_avg_pct": float(b_row["avg_saudization_pct"]),
            "industry_median_pct": float(b_row["median_saudization_pct"]),
            "trend": b_row["trend_direction"]
        }
    
    total = int(row["establishment_count"])
    compliant = int(row["platinum_count"]) + int(row["green_count"])
    
    return {
        "sector": sector,
        "establishment_count": total,
        "total_employees": int(row["total_employees"]),
        "total_saudi_employees": int(row["total_saudis"]),
        "avg_saudization_pct": float(row["avg_saudization_pct"]),
        "zone_distribution": {
            "platinum": int(row["platinum_count"]),
            "green": int(row["green_count"]),
            "yellow": int(row["yellow_count"]),
            "red": int(row["red_count"])
        },
        "compliance_rate_pct": round(compliant / total * 100, 1) if total > 0 else 0,
        "benchmark": benchmark
    }

# COMMAND ----------

def get_compliance_alerts(
    establishment_name: str = None,
    severity: str = None,
    unread_only: bool = True
) -> Dict[str, Any]:
    """
    Get compliance alerts for an establishment or all establishments.
    
    Args:
        establishment_name: Optional - filter by establishment name
        severity: Optional - filter by severity (High, Medium, Low)
        unread_only: If True, only return unread alerts
        
    Returns:
        List of alerts with details
    """
    
    conditions = []
    
    if establishment_name:
        conditions.append(f"LOWER(establishment_name) LIKE LOWER('%{establishment_name}%')")
    if severity:
        conditions.append(f"severity = '{severity}'")
    if unread_only:
        conditions.append("is_read = FALSE")
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    df = spark.sql(f"""
        SELECT 
            alert_id,
            establishment_name,
            alert_type,
            severity,
            message,
            recommended_action,
            created_date
        FROM {BRONZE_SCHEMA}.raw_compliance_alerts
        {where_clause}
        ORDER BY 
            CASE severity WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END,
            created_date DESC
    """)
    
    alerts = []
    for row in df.collect():
        alerts.append({
            "alert_id": row["alert_id"],
            "establishment": row["establishment_name"],
            "type": row["alert_type"],
            "severity": row["severity"],
            "message": row["message"],
            "recommended_action": row["recommended_action"],
            "created_date": str(row["created_date"])
        })
    
    return {
        "filters": {
            "establishment": establishment_name,
            "severity": severity,
            "unread_only": unread_only
        },
        "alert_count": len(alerts),
        "alerts": alerts
    }

# COMMAND ----------

def get_overall_summary() -> Dict[str, Any]:
    """
    Get high-level summary of all establishments.
    
    Returns:
        Overall statistics and zone distribution
    """
    
    df = spark.sql(f"""
        SELECT 
            COUNT(*) as total_establishments,
            SUM(total_employees) as total_workforce,
            SUM(saudi_employees) as total_saudis,
            ROUND(SUM(saudi_employees) * 100.0 / SUM(total_employees), 2) as overall_saudization_pct,
            SUM(CASE WHEN nitaqat_zone = 'Platinum' THEN 1 ELSE 0 END) as platinum,
            SUM(CASE WHEN nitaqat_zone = 'High_Green' THEN 1 ELSE 0 END) as high_green,
            SUM(CASE WHEN nitaqat_zone = 'Mid_Green' THEN 1 ELSE 0 END) as mid_green,
            SUM(CASE WHEN nitaqat_zone = 'Low_Green' THEN 1 ELSE 0 END) as low_green,
            SUM(CASE WHEN nitaqat_zone = 'Yellow' THEN 1 ELSE 0 END) as yellow,
            SUM(CASE WHEN nitaqat_zone = 'Red' THEN 1 ELSE 0 END) as red
        FROM {BRONZE_SCHEMA}.raw_establishments
    """)
    
    row = df.collect()[0]
    
    total = int(row["total_establishments"])
    compliant = int(row["platinum"]) + int(row["high_green"]) + int(row["mid_green"]) + int(row["low_green"])
    non_compliant = int(row["yellow"]) + int(row["red"])
    
    return {
        "total_establishments": total,
        "total_workforce": int(row["total_workforce"]),
        "total_saudi_employees": int(row["total_saudis"]),
        "overall_saudization_pct": float(row["overall_saudization_pct"]),
        "zone_distribution": {
            "platinum": int(row["platinum"]),
            "high_green": int(row["high_green"]),
            "mid_green": int(row["mid_green"]),
            "low_green": int(row["low_green"]),
            "yellow": int(row["yellow"]),
            "red": int(row["red"])
        },
        "compliance_summary": {
            "compliant": compliant,
            "non_compliant": non_compliant,
            "compliance_rate_pct": round(compliant / total * 100, 1)
        }
    }

# COMMAND ----------

def compare_establishments(establishment_names: list) -> Dict[str, Any]:
    """
    Compare multiple establishments side by side.
    
    Args:
        establishment_names: List of establishment names to compare
        
    Returns:
        Comparison table with key metrics for each establishment
    """
    
    comparisons = []
    
    for name in establishment_names:
        status = get_establishment_status(name)
        if "error" not in status:
            comparisons.append({
                "name": status["name"],
                "sector": status["sector"],
                "total_employees": status["total_employees"],
                "saudi_employees": status["saudi_employees"],
                "saudization_pct": status["saudization_pct"],
                "nitaqat_zone": status["nitaqat_zone"],
                "compliance_status": status["compliance_status"]
            })
    
    if not comparisons:
        return {"error": "No matching establishments found"}
    
    # Find best and worst
    best = max(comparisons, key=lambda x: x["saudization_pct"])
    worst = min(comparisons, key=lambda x: x["saudization_pct"])
    
    return {
        "establishments_compared": len(comparisons),
        "best_performer": best["name"],
        "worst_performer": worst["name"],
        "comparison": comparisons
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Functions as Unity Catalog Tools

# COMMAND ----------

# Create schema for functions
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.nitaqat_tools")

# COMMAND ----------

# Register get_establishment_status
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
print("✓ Registered: nitaqat_tools.get_establishment_status")

# COMMAND ----------

# Register get_establishments_by_zone
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.nitaqat_tools.get_establishments_by_zone(zone STRING)
RETURNS TABLE (
    name STRING,
    sector STRING,
    region STRING,
    total_employees INT,
    saudi_employees INT,
    saudization_pct DOUBLE
)
RETURN
    SELECT 
        name,
        sector,
        region,
        total_employees,
        saudi_employees,
        ROUND(saudization_rate * 100, 2) as saudization_pct
    FROM {BRONZE_SCHEMA}.raw_establishments
    WHERE nitaqat_zone = zone
    ORDER BY saudization_rate ASC
""")
print("✓ Registered: nitaqat_tools.get_establishments_by_zone")

# COMMAND ----------

# Register get_sector_summary
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.nitaqat_tools.get_sector_summary(sector_name STRING)
RETURNS TABLE (
    sector STRING,
    establishment_count LONG,
    total_employees LONG,
    total_saudis LONG,
    avg_saudization_pct DOUBLE,
    platinum_count LONG,
    green_count LONG,
    yellow_count LONG,
    red_count LONG
)
RETURN
    SELECT 
        '{BRONZE_SCHEMA}' as sector,
        COUNT(*) as establishment_count,
        SUM(total_employees) as total_employees,
        SUM(saudi_employees) as total_saudis,
        ROUND(AVG(saudization_rate * 100), 2) as avg_saudization_pct,
        SUM(CASE WHEN nitaqat_zone = 'Platinum' THEN 1 ELSE 0 END) as platinum_count,
        SUM(CASE WHEN nitaqat_zone LIKE '%Green%' THEN 1 ELSE 0 END) as green_count,
        SUM(CASE WHEN nitaqat_zone = 'Yellow' THEN 1 ELSE 0 END) as yellow_count,
        SUM(CASE WHEN nitaqat_zone = 'Red' THEN 1 ELSE 0 END) as red_count
    FROM {BRONZE_SCHEMA}.raw_establishments
    WHERE sector = sector_name
""")
print("✓ Registered: nitaqat_tools.get_sector_summary")

# COMMAND ----------

# Register get_employees_expiring_soon
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.nitaqat_tools.get_saudi_employees_expiring(establishment_name STRING, days_ahead INT)
RETURNS TABLE (
    employee_id STRING,
    job_title STRING,
    department STRING,
    salary_sar INT,
    contract_end_date DATE,
    days_until_expiry INT
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
print("✓ Registered: nitaqat_tools.get_saudi_employees_expiring")

# COMMAND ----------

# Register get_compliance_alerts
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.nitaqat_tools.get_compliance_alerts(establishment_name STRING)
RETURNS TABLE (
    alert_type STRING,
    severity STRING,
    message STRING,
    recommended_action STRING,
    created_date TIMESTAMP
)
RETURN
    SELECT 
        alert_type,
        severity,
        message,
        recommended_action,
        created_date
    FROM {BRONZE_SCHEMA}.raw_compliance_alerts
    WHERE LOWER(establishment_name) LIKE CONCAT('%', LOWER(establishment_name), '%')
      AND is_read = FALSE
    ORDER BY 
        CASE severity WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END,
        created_date DESC
""")
print("✓ Registered: nitaqat_tools.get_compliance_alerts")

# COMMAND ----------

# Register get_overall_summary
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.nitaqat_tools.get_overall_summary()
RETURNS TABLE (
    total_establishments LONG,
    total_workforce LONG,
    total_saudis LONG,
    overall_saudization_pct DOUBLE,
    platinum_count LONG,
    high_green_count LONG,
    mid_green_count LONG,
    low_green_count LONG,
    yellow_count LONG,
    red_count LONG
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
        SUM(CASE WHEN nitaqat_zone = 'Red' THEN 1 ELSE 0 END) as red_count
    FROM {BRONZE_SCHEMA}.raw_establishments
""")
print("✓ Registered: nitaqat_tools.get_overall_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Functions Created
# MAGIC 
# MAGIC | Function | Description |
# MAGIC |----------|-------------|
# MAGIC | `get_establishment_status` | Get current status of an establishment |
# MAGIC | `get_nitaqat_thresholds` | Get zone thresholds for sector/size |
# MAGIC | `calculate_saudis_needed_for_zone` | Calculate hiring needs for target zone |
# MAGIC | `get_employees_by_establishment` | Get employee list with filters |
# MAGIC | `simulate_workforce_change` | What-if scenario simulation |
# MAGIC | `generate_hiring_plan` | Create hiring timeline with budget |
# MAGIC | `get_establishments_by_zone` | List establishments in a zone |
# MAGIC | `get_sector_summary` | Sector-level statistics |
# MAGIC | `get_compliance_alerts` | Get active alerts |
# MAGIC | `get_overall_summary` | High-level summary |
# MAGIC | `compare_establishments` | Side-by-side comparison |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test All Functions

# COMMAND ----------

print("=" * 60)
print("TESTING ALL FUNCTIONS")
print("=" * 60)

# Test each function
print("\n1. get_establishment_status:")
print(json.dumps(get_establishment_status("Al Faris"), indent=2, default=str))

print("\n2. get_nitaqat_thresholds:")
print(json.dumps(get_nitaqat_thresholds("Retail", "Medium"), indent=2))

print("\n3. calculate_saudis_needed_for_zone:")
print(json.dumps(calculate_saudis_needed_for_zone("Al Faris", "High_Green"), indent=2))

print("\n4. simulate_workforce_change:")
print(json.dumps(simulate_workforce_change("Al Faris", saudi_hires=5, non_saudi_departures=3), indent=2))

print("\n5. get_establishments_by_zone:")
result = get_establishments_by_zone("Red")
print(f"Red zone count: {result['count']}")

print("\n6. get_sector_summary:")
print(json.dumps(get_sector_summary("Information Technology"), indent=2))

print("\n7. get_overall_summary:")
print(json.dumps(get_overall_summary(), indent=2))
