#!/usr/bin/env python3
"""
Rebuild data.json from Coupler.io data flows.

This script queries each Coupler.io data flow via their REST API,
aggregates the data, and writes a fresh data.json for the dashboard.

Usage:
  COUPLER_API_TOKEN=your_token python build_data.py

The token can be generated in Coupler.io > Settings > API.
"""

import json
import os
import sys
from datetime import datetime, timedelta
import requests

BASE_URL = "https://app.coupler.io/api/v1"
TOKEN = os.environ.get("COUPLER_API_TOKEN", "")

# Data flow IDs (from Coupler.io)
FLOWS = {
    "line_items": "c66818e4-0fd4-4ae4-a539-c1a465466d6d",
    "inventory": "636c31d1-5422-4fea-9bbb-ed754d9b4dde",
    "ga4": "062888d4-0c2f-467f-95de-a8cdb0403675",
    "gsc": "e4ca2d8e-e245-42e1-8586-2c65139b3f98",
    "meta": "61ef6e74-686b-488f-8103-b519561ed1a6",
    "google": "9bcb59a1-9e17-4373-838d-f91569f6f010",
    "klaviyo_campaigns": "8b920d8f-e988-4cfd-98b6-62443fae8240",
    "klaviyo_flows": "8ee8fbab-f42a-456d-86dd-0adbbb6b22da",
    "klaviyo_metrics": "6a4456fe-99a3-4c8f-8345-2e28bd29ee3d",
    "products": "6693901c-0d36-4fdb-8096-d656c7ba33fe",
}

HEADERS = {}


def set_auth():
    global HEADERS
    if not TOKEN:
        print("WARNING: No COUPLER_API_TOKEN set. Using existing data.json.")
        sys.exit(0)
    HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def get_latest_execution(flow_id):
    """Get the latest successful execution ID for a data flow."""
    resp = requests.get(f"{BASE_URL}/dataflows/{flow_id}", headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()
    return data.get("last_successful_execution_id")


def query_data(execution_id, sql):
    """Run a SQL query against a Coupler.io execution."""
    resp = requests.post(
        f"{BASE_URL}/executions/{execution_id}/query",
        headers=HEADERS,
        json={"query": sql},
    )
    resp.raise_for_status()
    return resp.json()


def get_current_week():
    """Calculate the most recent completed Sun-Sat week."""
    today = datetime.utcnow().date()
    # Find last Saturday
    days_since_sat = (today.weekday() + 2) % 7
    if days_since_sat == 0:
        last_sat = today
    else:
        last_sat = today - timedelta(days=days_since_sat)
    week_end = last_sat
    week_start = week_end - timedelta(days=6)

    # Calculate week number (weeks since start of year, Sun-Sat)
    jan1 = datetime(today.year, 1, 1).date()
    # Find first Sunday of year
    first_sun = jan1 + timedelta(days=(6 - jan1.weekday()) % 7)
    if week_start < first_sun:
        week_num = 1
    else:
        week_num = ((week_start - first_sun).days // 7) + 1

    return week_num, week_start, week_end


def build_kpis(exec_id, week_start, week_end):
    """Build KPI data from line items."""
    ws = week_start.isoformat()
    we = (week_end + timedelta(days=1)).isoformat()

    rows = query_data(exec_id, f"""
        SELECT
            COUNT(DISTINCT col_1) as orders,
            ROUND(SUM(col_14), 0) as revenue,
            SUM(col_12) as units,
            COUNT(DISTINCT CASE WHEN col_21 = 1 THEN col_7 END) as new_customers,
            COUNT(DISTINCT CASE WHEN col_21 > 1 THEN col_7 END) as repeat_customers
        FROM data
        WHERE col_3 = 'PAID' AND col_4 >= '{ws}' AND col_4 < '{we}'
    """)

    if not rows:
        return {}

    r = rows[0]
    orders = r["orders"] or 0
    revenue = r["revenue"] or 0
    units = r["units"] or 0
    new_c = r["new_customers"] or 0
    repeat_c = r["repeat_customers"] or 0
    total_c = new_c + repeat_c

    return {
        "revenue": revenue,
        "orders": orders,
        "aov": round(revenue / orders, 2) if orders else 0,
        "units": units,
        "units_per_order": round(units / orders, 2) if orders else 0,
        "new_customers": new_c,
        "repeat_rate": round(repeat_c / total_c * 100, 1) if total_c else 0,
    }


def build_daily(exec_id, week_start, week_end):
    """Build daily breakdown from line items."""
    ws = week_start.isoformat()
    we = (week_end + timedelta(days=1)).isoformat()

    return query_data(exec_id, f"""
        SELECT
            CASE strftime('%w', col_4)
                WHEN '0' THEN 'Sun' WHEN '1' THEN 'Mon' WHEN '2' THEN 'Tue'
                WHEN '3' THEN 'Wed' WHEN '4' THEN 'Thu' WHEN '5' THEN 'Fri'
                WHEN '6' THEN 'Sat'
            END || ' ' || substr(col_4, 6, 2) || '/' || substr(col_4, 9, 2) as day,
            COUNT(DISTINCT col_1) as orders,
            ROUND(SUM(col_14), 0) as revenue,
            SUM(col_12) as units,
            ROUND(SUM(col_14) / COUNT(DISTINCT col_1), 2) as aov
        FROM data
        WHERE col_3 = 'PAID' AND col_4 >= '{ws}' AND col_4 < '{we}'
        GROUP BY date(col_4)
        ORDER BY date(col_4)
    """)


def build_vendors(exec_id, week_start, week_end):
    """Build vendor/franchise breakdown."""
    ws = week_start.isoformat()
    we = (week_end + timedelta(days=1)).isoformat()

    return query_data(exec_id, f"""
        SELECT
            col_11 as name,
            SUM(col_12) as units,
            ROUND(SUM(col_14), 0) as revenue,
            COUNT(DISTINCT col_1) as orders,
            ROUND(SUM(col_14) / SUM(col_12), 2) as avg_price
        FROM data
        WHERE col_3 = 'PAID' AND col_4 >= '{ws}' AND col_4 < '{we}'
        GROUP BY col_11
        ORDER BY revenue DESC
    """)


def build_products(exec_id, week_start, week_end):
    """Build top products."""
    ws = week_start.isoformat()
    we = (week_end + timedelta(days=1)).isoformat()

    return query_data(exec_id, f"""
        SELECT
            col_9 as name,
            col_11 as vendor,
            SUM(col_12) as units,
            ROUND(SUM(col_14), 0) as revenue,
            ROUND(SUM(col_14) / SUM(col_12), 0) as price
        FROM data
        WHERE col_3 = 'PAID' AND col_4 >= '{ws}' AND col_4 < '{we}'
        GROUP BY col_9, col_11
        ORDER BY units DESC
        LIMIT 20
    """)


def build_segments(exec_id, week_start, week_end):
    """Build customer segments."""
    ws = week_start.isoformat()
    we = (week_end + timedelta(days=1)).isoformat()

    return query_data(exec_id, f"""
        SELECT
            CASE
                WHEN col_21 = 1 THEN '1st Order'
                WHEN col_21 = 2 THEN '2nd Order'
                WHEN col_21 = 3 THEN '3rd Order'
                WHEN col_21 BETWEEN 4 AND 5 THEN '4-5 Orders'
                WHEN col_21 BETWEEN 6 AND 10 THEN '6-10 Orders'
                WHEN col_21 > 10 THEN '11+ Orders'
                ELSE 'Unknown'
            END as segment,
            COUNT(DISTINCT col_1) as orders,
            ROUND(SUM(col_14), 0) as revenue,
            ROUND(SUM(col_14) / COUNT(DISTINCT col_1), 2) as aov
        FROM data
        WHERE col_3 = 'PAID' AND col_4 >= '{ws}' AND col_4 < '{we}'
        GROUP BY segment
        ORDER BY MIN(col_21)
    """)


def build_meta_campaigns(exec_id, week_start, week_end):
    """Build Meta campaign breakdown."""
    ws = week_start.isoformat()
    we = (week_end + timedelta(days=1)).isoformat()

    return query_data(exec_id, f"""
        SELECT
            col_1 as campaign,
            ROUND(SUM(col_3), 0) as spend,
            SUM(col_13) as purchases,
            ROUND(CASE WHEN SUM(col_3) > 0 THEN SUM(col_15) / SUM(col_3) ELSE 0 END, 2) as roas,
            ROUND(CASE WHEN SUM(col_13) > 0 THEN SUM(col_3) / SUM(col_13) ELSE 0 END, 2) as cpa
        FROM data
        WHERE col_0 >= '{ws}' AND col_0 < '{we}'
        GROUP BY col_1
        HAVING spend > 0
        ORDER BY spend DESC
    """)


def build_google_campaigns(exec_id, week_start, week_end):
    """Build Google campaign breakdown."""
    ws = week_start.isoformat()
    we = (week_end + timedelta(days=1)).isoformat()

    return query_data(exec_id, f"""
        SELECT
            col_3 as campaign,
            ROUND(SUM(col_9), 0) as spend,
            ROUND(SUM(col_16), 1) as conversions,
            ROUND(CASE WHEN SUM(col_16) > 0 THEN SUM(col_9) / SUM(col_16) ELSE 0 END, 2) as cpa,
            SUM(col_14) as clicks
        FROM data
        WHERE col_2 >= '{ws}' AND col_2 < '{we}'
        GROUP BY col_3
        HAVING spend > 0
        ORDER BY spend DESC
    """)


def build_gsc_keywords(exec_id, week_start, week_end):
    """Build top GSC keywords."""
    ws = week_start.isoformat()
    we = (week_end + timedelta(days=1)).isoformat()

    return query_data(exec_id, f"""
        SELECT
            col_2 as query,
            SUM(col_5) as clicks,
            SUM(col_6) as impressions,
            ROUND(AVG(col_8), 1) as position
        FROM data
        WHERE col_1 >= '{ws}' AND col_1 < '{we}'
            AND col_2 NOT LIKE '%heroes%'
            AND col_2 NOT LIKE '%villains%'
            AND col_2 NOT LIKE '%heroesandvillains%'
        GROUP BY col_2
        ORDER BY clicks DESC
        LIMIT 12
    """)


def build_channels(exec_id, week_start, week_end):
    """Build GA4 channel mix."""
    ws = week_start.isoformat()
    we = (week_end + timedelta(days=1)).isoformat()

    return query_data(exec_id, f"""
        SELECT
            col_17 as name,
            ROUND(SUM(col_12), 0) as sessions,
            ROUND(SUM(col_15), 0) as revenue
        FROM data
        WHERE col_5 >= '{ws}' AND col_5 < '{we}'
        GROUP BY col_17
        ORDER BY revenue DESC
        LIMIT 10
    """)


def build_inventory(exec_id):
    """Build inventory overview."""
    rows = query_data(exec_id, """
        SELECT
            COUNT(DISTINCT col_0) as total_variants,
            SUM(col_50) as on_hand,
            SUM(col_56) as available,
            SUM(col_58) as committed,
            SUM(CASE WHEN col_56 = 0 THEN 1 ELSE 0 END) as oos_variants,
            SUM(CASE WHEN col_56 BETWEEN 1 AND 5 THEN 1 ELSE 0 END) as low_stock
        FROM data
        WHERE col_19 = '"Bluespoke"'
    """)
    return rows[0] if rows else {}


def main():
    set_auth()

    week_num, week_start, week_end = get_current_week()
    week_label = f"WK{week_num:02d}"

    print(f"Building data for {week_label}: {week_start} to {week_end}")

    # Get latest execution IDs for each flow
    executions = {}
    for name, flow_id in FLOWS.items():
        try:
            exec_id = get_latest_execution(flow_id)
            if exec_id:
                executions[name] = exec_id
                print(f"  {name}: {exec_id}")
            else:
                print(f"  {name}: No successful execution found")
        except Exception as e:
            print(f"  {name}: Error getting execution: {e}")

    data = {"meta": {
        "week_number": week_num,
        "week_label": week_label,
        "date_range": f"{week_start.strftime('%b %d')} – {week_end.strftime('%b %d, %Y')}",
        "year": week_start.year,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }}

    # Build each section
    if "line_items" in executions:
        eid = executions["line_items"]
        print("Building KPIs...")
        data["kpis"] = build_kpis(eid, week_start, week_end)
        print("Building daily breakdown...")
        data["daily_2026"] = build_daily(eid, week_start, week_end)
        print("Building vendor breakdown...")
        data["vendors"] = build_vendors(eid, week_start, week_end)
        print("Building top products...")
        data["products"] = build_products(eid, week_start, week_end)
        print("Building customer segments...")
        data["segments"] = build_segments(eid, week_start, week_end)

    if "meta" in executions:
        print("Building Meta campaigns...")
        data["meta_campaigns"] = build_meta_campaigns(
            executions["meta"], week_start, week_end
        )

    if "google" in executions:
        print("Building Google campaigns...")
        data["google_campaigns"] = build_google_campaigns(
            executions["google"], week_start, week_end
        )

    if "gsc" in executions:
        print("Building GSC keywords...")
        data["gsc_keywords"] = build_gsc_keywords(
            executions["gsc"], week_start, week_end
        )

    if "ga4" in executions:
        print("Building GA4 channels...")
        data["channels"] = build_channels(executions["ga4"], week_start, week_end)

    if "inventory" in executions:
        print("Building inventory...")
        data["inventory"] = build_inventory(executions["inventory"])

    # Write data.json
    with open("data.json", "w") as f:
        json.dump(data, f, indent=2)

    print(f"\ndata.json written successfully ({len(json.dumps(data))} bytes)")


if __name__ == "__main__":
    main()
