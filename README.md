# Restaurant Analytics Platform 
A Databricks Lakehouse project that builds a star schema and aggregate marts for a multi-restaurant analytics platform. The Gold layer sits on top of a medallion architecture (Bronze → Silver → Gold) and delivers business-ready dimensions, facts, and pre-aggregated KPIs for BI and reporting.

# Overview
This project transforms cleaned data from the Silver layer (restaurant_dev.silver) into an analytics-optimized Gold layer (restaurant_dev.gold) using Databricks Lakeflow Declarative Pipelines.
The Gold layer provides:
•	Conformed dimensions — reusable across all facts
•	Fact tables — transactional and operational grains
•	Aggregate marts — pre-computed KPIs for revenue, customers, menu, staffing, delivery, and operations
# Data Model
## Dimensions
Table	Description
dim_date	Calendar 2023–2026 with UK holiday and weekend flags
dim_restaurant	Restaurant location, capacity, and size band
dim_customer	Customer demographics + RFM segmentation + CLV
dim_menu_item	Menu item pricing, cost, margin, and dietary flags
dim_employee	Employee role, rate, tenure, and active status
## Facts
Table	Grain	Description
fct_orders	1 row per order	Order header with totals and status flags
fct_order_items	1 row per order line	Line-level revenue, cost, and margin
fct_delivery	1 row per delivery	Delivery duration, distance, and on-time flags
fct_inventory_daily	restaurant × item × date	Stock movements and waste %
fct_daily_ops	restaurant × date	Customers, orders, staff, utilities, revenue
## Aggregate Marts
Table	Description
agg_revenue_monthly	Monthly revenue, orders, AOV, margin
agg_revenue_by_category	Revenue and margin by category
agg_restaurant_performance	KPIs per restaurant
agg_hourly_demand	Orders by day-of-week and hour
agg_customer_cohorts	Monthly retention by signup cohort
agg_customer_lifetime	CLV and RFM distribution
agg_item_performance	Units, revenue, margin per item
agg_menu_engineering	Stars / Puzzles / Plowhorses / Dogs classification
agg_ops_costs	Monthly operating costs per restaurant
agg_staffing_efficiency	Headcount, turnover, payroll KPIs
agg_delivery_by_condition	On-time % by traffic and weather
agg_driver_performance	Per-driver trips and on-time %
________________________________________
# Dashboard
A pre-built Databricks AI/BI Dashboard is included as dashboard.json, providing visualizations on top of the Gold layer.
Importing the Dashboard
1.	In Databricks, go to Dashboards → Create Dashboard → Import from file
2.	Upload dashboard.json
3.	Point the datasets to restaurant_dev.gold
4.	Publish
# Dashboard Coverage
•	Revenue — monthly trends, category breakdown, restaurant performance
•	Customers — RFM segments, cohorts, CLV
•	Menu — item performance, menu engineering matrix
•	Operations — staffing efficiency, ops costs, hourly demand
•	Delivery — on-time rates, driver performance, traffic/weather impact
________________________________________
#  Business Logic Highlights
•	RFM Segmentation — customers scored 1–5 on Recency, Frequency, Monetary using ntile(5)
•	Customer Lifetime Value — avg order value × total orders
•	Menu Engineering — items classified using median thresholds for popularity and profitability
•	UK Holiday Flags — Christmas, New Year, Easter, and May bank holidays
________________________________________
## Tech Stack
Layer	Technology
Platform	Databricks (Unity Catalog)
Pipeline Framework	Lakeflow Declarative Pipelines
Language	Python / PySpark
Storage	Delta Lake
Visualization	Databricks AI/BI Dashboards

# Getting Started
## Prerequisites
•	Databricks workspace with Unity Catalog
•	Silver tables populated in restaurant_dev.silver
## Deployment
1.	Import gold_layer.py as a notebook or pipeline source file
2.	Create a Lakeflow Declarative Pipeline targeting restaurant_dev.gold
3.	Trigger a full refresh
4.	Import dashboard.json as a new AI/BI Dashboard
________________________________________
## Usage
sql
-- Monthly revenue trend
SELECT year_month, revenue, aov, gross_margin_pct
FROM restaurant_dev.gold.agg_revenue_monthly
ORDER BY year_month;

-- Top menu items per restaurant
SELECT restaurant_id, item_id, units_sold, revenue, margin_pct
FROM restaurant_dev.gold.agg_item_performance
ORDER BY restaurant_id, revenue DESC;
________________________________________
# Conventions
•	dim_* — dimensions | fct_* — facts | agg_* — aggregates
•	Monetary values rounded to 2 dp; percentages to 2 dp
•	All views are declarative and safe to rerun
________________________________________
# License
Internal project — © Restaurant Analytics Platform.

