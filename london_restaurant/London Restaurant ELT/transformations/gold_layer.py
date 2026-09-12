from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, lit, when, trim, upper, initcap, lower,
    to_date, to_timestamp, year, month, dayofmonth, dayofweek,
    quarter, weekofyear, date_format, datediff, months_between,
    current_date, current_timestamp,
    sum as _sum, avg, count, countDistinct, min as _min, max as _max,
    round as _round, coalesce, greatest, least, expr,
    row_number, dense_rank, ntile, percentile_approx,
    concat_ws, split, regexp_replace
)
from pyspark.sql.window import Window

SILVER = "restaurant_dev.silver"
GOLD   = "restaurant_dev.gold"

# =========================================================
# DIMENSIONS
# =========================================================

@dp.materialized_view(
    name=f"{GOLD}.dim_date",
    comment="Date dimension: 2023-2026 calendar with UK-relevant attributes"
)
def dim_date():
    return (
        spark.sql("""
            SELECT explode(sequence(
                to_date('2023-01-01'),
                to_date('2026-12-31'),
                interval 1 day
            )) AS date
        """)
        .withColumn("date_key",        date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year",            year("date"))
        .withColumn("quarter",         quarter("date"))
        .withColumn("month",           month("date"))
        .withColumn("month_name",      date_format("date", "MMMM"))
        .withColumn("month_short",     date_format("date", "MMM"))
        .withColumn("week_of_year",    weekofyear("date"))
        .withColumn("day_of_month",    dayofmonth("date"))
        .withColumn("day_of_week",     dayofweek("date"))
        .withColumn("day_name",        date_format("date", "EEEE"))
        .withColumn("day_short",       date_format("date", "EEE"))
        .withColumn("is_weekend",      when(dayofweek("date").isin(1, 7), True).otherwise(False))
        .withColumn("is_holiday",
            when(
                ((month("date") == 12) & (dayofmonth("date").isin(25, 26, 31))) |
                ((month("date") == 1)  & (dayofmonth("date") == 1)) |
                ((month("date") == 4)  & (dayofmonth("date").isin(1, 7, 8, 25))) |
                ((month("date") == 5)  & (dayofmonth("date").isin(1, 6, 8, 26, 27))),
                True
            ).otherwise(False))
        .withColumn("year_month",      date_format("date", "yyyy-MM"))
        .withColumn("year_quarter",    concat_ws("-Q", year("date"), quarter("date")))
    )


@dp.materialized_view(
    name=f"{GOLD}.dim_restaurant",
    comment="Restaurant dimension with geo + capacity attributes"
)
def dim_restaurant():
    return (
        spark.read.table(f"{SILVER}.restaurants")
        .select(
            col("restaurant_id").cast("int").alias("restaurant_key"),
            col("restaurant_id").cast("int").alias("restaurant_id"),
            col("restaurant_name"),
            col("address"),
            col("postcode"),
            upper(col("area")).alias("area"),
            col("opening_date"),
            col("seating_capacity").cast("int"),
            col("delivery_radius_miles").cast("int"),
            col("phone_number"),
        )
        .withColumn("years_in_operation",
            _round(months_between(current_date(), col("opening_date")) / 12, 1))
        .withColumn("size_band",
            when(col("seating_capacity") < 60, "Small")
            .when(col("seating_capacity") < 80, "Medium")
            .otherwise("Large"))
    )


@dp.materialized_view(
    name=f"{GOLD}.dim_customer",
    comment="Customer dimension with RFM segmentation and lifetime metrics"
)
def dim_customer():
    customers = (
        spark.read.table(f"{SILVER}.customers")
        .select(
            col("customer_id").cast("int").alias("customer_key"),
            col("customer_id").cast("int").alias("customer_id"),
            col("first_name"),
            col("last_name"),
            concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
            lower(col("email")).alias("email"),
            col("phone"),
            upper(col("postcode")).alias("postcode"),
            to_date(col("join_date")).alias("join_date"),
            initcap(col("customer_segment")).alias("customer_segment"),
            initcap(col("preferred_category")).alias("preferred_category"),
            initcap(col("preferred_order_type")).alias("preferred_order_type"),
            col("avg_order_value").cast("double"),
            col("is_active").cast("boolean"),
        )
    )

    orders_agg = (
        spark.read.table(f"{SILVER}.orders")
        .filter(col("order_status") == "Completed")
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("_orders_cnt"),
            _sum("total_amount").alias("_total_spent"),
            avg("total_amount").alias("_avg_spent"),
            _max(to_date("order_date")).alias("_last_order_date"),
            _min(to_date("order_date")).alias("_first_order_date"),
        )
    )

    joined = customers.join(orders_agg, on="customer_id", how="left")

    joined = joined.withColumn(
        "_recency_days",
        datediff(current_date(), coalesce(col("_last_order_date"), col("join_date")))
    )

    w_r = Window.orderBy(col("_recency_days").asc())
    w_f = Window.orderBy(col("_orders_cnt").desc_nulls_last())
    w_m = Window.orderBy(col("_total_spent").desc_nulls_last())

    joined = (
        joined
        .withColumn("_r_score", when(col("_orders_cnt").isNull(), lit(1)).otherwise(ntile(5).over(w_r)))
        .withColumn("_f_score", when(col("_orders_cnt").isNull(), lit(1)).otherwise(ntile(5).over(w_f)))
        .withColumn("_m_score", when(col("_total_spent").isNull(), lit(1)).otherwise(ntile(5).over(w_m)))
    )

    joined = joined.withColumn(
        "rfm_segment",
        when((col("_r_score") >= 4) & (col("_f_score") >= 4) & (col("_m_score") >= 4), "Champion")
        .when((col("_r_score") >= 3) & (col("_f_score") >= 4), "Loyal")
        .when((col("_r_score") >= 4) & (col("_f_score") <= 2), "New / Promising")
        .when((col("_r_score") <= 2) & (col("_f_score") >= 4), "At Risk")
        .when((col("_r_score") <= 2) & (col("_f_score") <= 2), "Lost")
        .otherwise("Need Attention")
    )

    return (
        joined
        .withColumn("total_orders",  coalesce(col("_orders_cnt"), lit(0)).cast("int"))
        .withColumn("total_spent",   _round(coalesce(col("_total_spent"), lit(0.0)), 2))
        .withColumn("avg_order_value_actual", _round(coalesce(col("_avg_spent"), lit(0.0)), 2))
        .withColumn("customer_lifetime_value",
            _round(coalesce(col("_avg_spent"), col("avg_order_value")) * coalesce(col("_orders_cnt"), lit(0)), 2))
        .withColumn("first_order_date", col("_first_order_date"))
        .withColumn("last_order_date",  col("_last_order_date"))
        .withColumn("recency_days",     col("_recency_days"))
        .withColumn("r_score", col("_r_score"))
        .withColumn("f_score", col("_f_score"))
        .withColumn("m_score", col("_m_score"))
        .withColumn("rfm_score", concat_ws("", col("_r_score"), col("_f_score"), col("_m_score")))
        .drop("_orders_cnt", "_total_spent", "_avg_spent", "_last_order_date",
              "_first_order_date", "_recency_days", "_r_score", "_f_score", "_m_score")
    )


@dp.materialized_view(
    name=f"{GOLD}.dim_menu_item",
    comment="Menu item dimension with pricing, cost, dietary attributes"
)
def dim_menu_item():
    return (
        spark.read.table(f"{SILVER}.menu_items")
        .select(
            col("item_id").cast("int").alias("item_key"),
            col("item_id").cast("int").alias("item_id"),
            col("restaurant_id").cast("int").alias("restaurant_id"),
            col("item_name"),
            col("category"),
            col("sub_category"),
            col("price").cast("double"),
            col("cost_to_make").cast("double"),
            col("preparation_time_mins").cast("int"),
            col("is_vegetarian").cast("boolean"),
            col("is_vegan").cast("boolean"),
            col("is_gluten_free").cast("boolean"),
            col("calories").cast("int"),
        )
        .withColumn("gross_margin_gbp", _round(col("price") - col("cost_to_make"), 2))
        .withColumn("gross_margin_pct",
            when(col("price") > 0, _round((col("price") - col("cost_to_make")) / col("price") * 100, 2))
            .otherwise(lit(0.0)))
        .withColumn("dietary_flags",
            concat_ws(",",
                when(col("is_vegetarian"), lit("Vegetarian")),
                when(col("is_vegan"),      lit("Vegan")),
                when(col("is_gluten_free"), lit("Gluten-Free"))
            ))
    )


@dp.materialized_view(
    name=f"{GOLD}.dim_employee",
    comment="Employee dimension with role, rate, tenure"
)
def dim_employee():
    return (
        spark.read.table(f"{SILVER}.employees")
        .select(
            col("employee_id").cast("int").alias("employee_key"),
            col("employee_id").cast("int").alias("employee_id"),
            col("restaurant_id").cast("int").alias("restaurant_id"),
            col("first_name"),
            col("last_name"),
            concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
            regexp_replace(initcap(col("position")), "_", " ").alias("position"),
            to_date(col("hire_date")).alias("hire_date"),
            to_date(col("leave_date")).alias("leave_date"),
            col("salary_per_hour").cast("double"),
            col("hours_per_week").cast("int"),
            col("employment_status"),
            col("reason_for_leaving"),
            col("performance_rating").cast("double"),
        )
        .withColumn("tenure_months",
            _round(months_between(coalesce(col("leave_date"), current_date()), col("hire_date")), 1))
        .withColumn("is_active", when(col("employment_status") == "Active", True).otherwise(False))
        .withColumn("weekly_cost_gbp",
            _round(col("salary_per_hour") * col("hours_per_week"), 2))
    )


# =========================================================
# FACTS
# =========================================================

@dp.materialized_view(
    name=f"{GOLD}.fct_orders",
    comment="Fact: orders. Grain = 1 row per order."
)
def fct_orders():
    orders = (
        spark.read.table(f"{SILVER}.orders")
        .select(
            col("order_id").cast("int").alias("order_id"),
            col("restaurant_id").cast("int").alias("restaurant_id"),
            col("customer_id").cast("int").alias("customer_id"),
            to_date(col("order_date")).alias("order_date"),
            to_timestamp(col("order_time")).alias("order_time"),
            col("order_type"),
            col("payment_method"),
            col("order_status"),
            col("delivery_postcode"),
            col("delivery_distance_miles").cast("double"),
            col("delivery_time_mins").cast("int"),
            col("total_amount").cast("double"),
            col("customer_rating").cast("int"),
        )
    )

    items = (
        spark.read.table(f"{SILVER}.order_details")
        .groupBy("order_id")
        .agg(
            count("order_detail_id").alias("line_item_count"),
            _sum("quantity").alias("total_quantity"),
        )
    )

    df = (
        orders
        .join(items, on="order_id", how="left")
        .withColumn("line_item_count", coalesce(col("line_item_count"), lit(0)).cast("int"))
        .withColumn("total_quantity",  coalesce(col("total_quantity"),  lit(0)).cast("int"))
    )

    df = (
        df
        .withColumn("date_key", date_format("order_date", "yyyyMMdd").cast("int"))
        .withColumn("year", year("order_date"))
        .withColumn("month", month("order_date"))
        .withColumn("year_month", date_format("order_date", "yyyy-MM"))
        .withColumn("day_of_week", dayofweek("order_date"))
        .withColumn("hour_of_day", expr("hour(order_time)"))
    )

    df = (
        df
        .withColumn("is_completed", when(col("order_status") == "Completed", True).otherwise(False))
        .withColumn("is_cancelled", when(col("order_status") == "Cancelled", True).otherwise(False))
        .withColumn("is_refunded",  when(col("order_status") == "Refunded",  True).otherwise(False))
        .withColumn("is_delivery",  when(col("order_type")   == "Delivery",  True).otherwise(False))
        .withColumn("is_eat_in",    when(col("order_type")   == "Eat-in",    True).otherwise(False))
    )

    return df


@dp.materialized_view(
    name=f"{GOLD}.fct_order_items",
    comment="Fact: order line items with margin. Grain = 1 row per order detail."
)
def fct_order_items():
    details = (
        spark.read.table(f"{SILVER}.order_details")
        .select(
            col("order_detail_id").cast("int").alias("order_detail_id"),
            col("order_id").cast("int").alias("order_id"),
            col("item_id").cast("int").alias("item_id"),
            col("quantity").cast("int"),
            col("item_price_at_time").cast("double"),
            col("special_instructions"),
        )
    )

    orders = (
        spark.read.table(f"{SILVER}.orders")
        .select(
            col("order_id").cast("int").alias("order_id"),
            col("restaurant_id").cast("int").alias("restaurant_id"),
            col("customer_id").cast("int").alias("customer_id"),
            to_date(col("order_date")).alias("order_date"),
            col("order_type"),
            col("order_status"),
        )
    )

    menu = (
        spark.read.table(f"{SILVER}.menu_items")
        .select(
            col("item_id").cast("int").alias("item_id"),
            col("cost_to_make").cast("double").alias("cost_to_make"),
            col("category"),
            col("sub_category"),
        )
    )

    df = (
        details
        .join(orders, on="order_id", how="left")
        .join(menu,   on="item_id",  how="left")
    )

    df = (
        df
        .withColumn("line_revenue",
            _round(col("quantity") * col("item_price_at_time"), 2))
        .withColumn("line_cost",
            _round(col("quantity") * col("cost_to_make"), 2))
        .withColumn("line_margin",
            _round(col("quantity") * (col("item_price_at_time") - col("cost_to_make")), 2))
        .withColumn("date_key",
            date_format(col("order_date"), "yyyyMMdd").cast("int"))
    )

    return df


@dp.materialized_view(
    name=f"{GOLD}.fct_delivery",
    comment="Fact: delivery performance. Grain = 1 row per delivery."
)
def fct_delivery():
    df = (
        spark.read.table(f"{SILVER}.delivery_performance")
        .select(
            col("delivery_id").cast("int").alias("delivery_id"),
            col("order_id").cast("int").alias("order_id"),
            col("restaurant_id").cast("int").alias("restaurant_id"),
            col("driver_id").cast("int").alias("driver_id"),
            to_timestamp(col("dispatch_time")).alias("dispatch_time"),
            to_timestamp(col("delivery_time")).alias("delivery_time"),
            col("distance_miles").cast("double"),
            col("traffic_condition"),
            col("weather_condition"),
            col("delivery_status"),
        )
    )

    df = (
        df
        .withColumn("delivery_minutes",
            _round((col("delivery_time").cast("long") - col("dispatch_time").cast("long")) / 60.0, 2))
        .withColumn("delivery_date", to_date("dispatch_time"))
        .withColumn("date_key", date_format("dispatch_time", "yyyyMMdd").cast("int"))
        .withColumn("is_on_time",   when(col("delivery_status") == "On_Time",   True).otherwise(False))
        .withColumn("is_delayed",   when(col("delivery_status") == "Delayed",   True).otherwise(False))
        .withColumn("is_cancelled", when(col("delivery_status") == "Cancelled", True).otherwise(False))
    )

    return df


@dp.materialized_view(
    name=f"{GOLD}.fct_inventory_daily",
    comment="Fact: daily inventory movements. Grain = restaurant + item + date."
)
def fct_inventory_daily():
    df = (
        spark.read.table(f"{SILVER}.inventory")
        .select(
            col("inventory_id").cast("int").alias("inventory_id"),
            col("restaurant_id").cast("int").alias("restaurant_id"),
            to_date(col("date")).alias("date"),
            col("item_id").cast("int").alias("item_id"),
            col("opening_stock").cast("int"),
            col("received_stock").cast("int"),
            col("used_stock").cast("int"),
            coalesce(col("waste_stock"), lit(0)).cast("int").alias("waste_stock"),
            col("closing_stock").cast("int"),
            col("stock_value").cast("double"),
            coalesce(col("waste_value"), lit(0.0)).cast("double").alias("waste_value"),
        )
    )

    df = (
        df
        .withColumn("date_key", date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year_month", date_format("date", "yyyy-MM"))
        .withColumn("waste_pct",
            when((col("used_stock") + col("waste_stock")) > 0,
                 _round(col("waste_stock") / (col("used_stock") + col("waste_stock")) * 100, 2))
            .otherwise(lit(0.0)))
    )

    return df


@dp.materialized_view(
    name=f"{GOLD}.fct_daily_ops",
    comment="Fact: daily operations. Grain = restaurant + date."
)
def fct_daily_ops():
    df = (
        spark.read.table(f"{SILVER}.daily_operations")
        .select(
            col("operation_id").cast("int").alias("operation_id"),
            col("restaurant_id").cast("int").alias("restaurant_id"),
            to_date(col("date")).alias("date"),
            col("total_customers_eat_in").cast("int"),
            col("total_customers_delivery").cast("int"),
            col("total_orders").cast("int"),
            col("delivery_orders_count").cast("int"),
            col("eat_in_orders_count").cast("int"),
            col("average_wait_time_mins").cast("int"),
            col("staff_on_shift").cast("int"),
            col("electricity_cost").cast("double"),
            col("water_cost").cast("double"),
            col("gas_cost").cast("double"),
            col("maintenance_cost").cast("double"),
            col("other_bills_cost").cast("double"),
            col("daily_revenue").cast("double"),
        )
    )

    df = (
        df
        .withColumn("date_key", date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year_month", date_format("date", "yyyy-MM"))
        .withColumn("total_customers",
            col("total_customers_eat_in") + col("total_customers_delivery"))
        .withColumn("total_operating_cost",
            _round(col("electricity_cost") + col("water_cost") + col("gas_cost") +
                   col("maintenance_cost") + col("other_bills_cost"), 2))
        .withColumn("revenue_per_customer",
            when(col("total_customers") > 0,
                 _round(col("daily_revenue") / col("total_customers"), 2))
            .otherwise(lit(0.0)))
    )

    return df


# =========================================================
# AGGREGATES (BI MARTS)
# =========================================================

@dp.materialized_view(
    name=f"{GOLD}.agg_revenue_monthly",
    comment="Aggregate: monthly revenue, orders, AOV, margin"
)
def agg_revenue_monthly():
    orders = spark.read.table(f"{GOLD}.fct_orders").filter(col("is_completed"))
    items  = (
        spark.read.table(f"{GOLD}.fct_order_items")
        .groupBy("order_id")
        .agg(_sum("line_margin").alias("order_margin"),
             _sum("line_cost").alias("order_cost"))
    )

    df = (
        orders.join(items, on="order_id", how="left")
        .groupBy("year", "month", "year_month")
        .agg(
            count("order_id").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            _round(_sum("total_amount"), 2).alias("revenue"),
            _round(avg("total_amount"), 2).alias("aov"),
            _round(_sum("order_margin"), 2).alias("gross_margin"),
            _round(_sum("order_cost"), 2).alias("total_cogs"),
            _round(_sum("total_quantity"), 0).alias("items_sold"),
        )
        .withColumn("gross_margin_pct",
            when(col("revenue") > 0,
                 _round(col("gross_margin") / col("revenue") * 100, 2))
            .otherwise(lit(0.0)))
        .orderBy("year", "month")
    )
    return df


@dp.materialized_view(
    name=f"{GOLD}.agg_revenue_by_category",
    comment="Aggregate: revenue by menu category and sub-category"
)
def agg_revenue_by_category():
    df = (
        spark.read.table(f"{GOLD}.fct_order_items")
        .groupBy("category", "sub_category")
        .agg(
            _round(_sum("line_revenue"), 2).alias("revenue"),
            _round(_sum("line_cost"), 2).alias("cost"),
            _round(_sum("line_margin"), 2).alias("margin"),
            _sum("quantity").alias("units_sold"),
            count("order_detail_id").alias("line_items"),
        )
        .withColumn("margin_pct",
            when(col("revenue") > 0,
                 _round(col("margin") / col("revenue") * 100, 2))
            .otherwise(lit(0.0)))
    )
    return df


@dp.materialized_view(
    name=f"{GOLD}.agg_restaurant_performance",
    comment="Aggregate: KPIs per restaurant (revenue, orders, AOV, delivery mix)"
)
def agg_restaurant_performance():
    orders = spark.read.table(f"{GOLD}.fct_orders")
    items  = (
        spark.read.table(f"{GOLD}.fct_order_items")
        .groupBy("restaurant_id", "order_id")
        .agg(_sum("line_margin").alias("order_margin"))
        .groupBy("restaurant_id")
        .agg(_round(_sum("order_margin"), 2).alias("gross_margin"))
    )

    df = (
        orders
        .groupBy("restaurant_id")
        .agg(
            count("order_id").alias("total_orders"),
            _round(_sum("total_amount"), 2).alias("revenue"),
            _round(avg("total_amount"), 2).alias("aov"),
            countDistinct("customer_id").alias("unique_customers"),
            _sum(when(col("is_delivery"), 1).otherwise(0)).alias("delivery_orders"),
            _sum(when(col("is_eat_in"),   1).otherwise(0)).alias("eat_in_orders"),
            _round(avg(when(col("is_completed"), col("customer_rating"))), 2).alias("avg_rating"),
        )
        .join(items, on="restaurant_id", how="left")
        .withColumn("delivery_share_pct",
            when(col("total_orders") > 0,
                 _round(col("delivery_orders") / col("total_orders") * 100, 2))
            .otherwise(lit(0.0)))
        .withColumn("gross_margin_pct",
            when(col("revenue") > 0,
                 _round(col("gross_margin") / col("revenue") * 100, 2))
            .otherwise(lit(0.0)))
    )
    return df


@dp.materialized_view(
    name=f"{GOLD}.agg_hourly_demand",
    comment="Aggregate: order volume & revenue by day-of-week and hour"
)
def agg_hourly_demand():
    df = (
        spark.read.table(f"{GOLD}.fct_orders")
        .filter(col("is_completed"))
        .groupBy("day_of_week", "hour_of_day")
        .agg(
            count("order_id").alias("orders"),
            _round(_sum("total_amount"), 2).alias("revenue"),
            _round(avg("total_amount"), 2).alias("aov"),
        )
    )
    return df


@dp.materialized_view(
    name=f"{GOLD}.agg_customer_cohorts",
    comment="Aggregate: monthly retention by customer signup cohort"
)
def agg_customer_cohorts():
    customers = (
        spark.read.table(f"{GOLD}.dim_customer")
        .select("customer_id", "join_date")
        .withColumn("cohort_month", date_format("join_date", "yyyy-MM"))
    )

    orders = (
        spark.read.table(f"{GOLD}.fct_orders")
        .filter(col("is_completed"))
        .select("customer_id", "order_date")
        .withColumn("order_month", date_format("order_date", "yyyy-MM"))
    )

    df = (
        customers
        .join(orders, on="customer_id", how="inner")
        .groupBy("cohort_month", "order_month")
        .agg(countDistinct("customer_id").alias("active_customers"))
        .withColumn("months_since_join",
            (year(col("order_month").cast("date"))  - year(col("cohort_month").cast("date"))) * 12 +
            (month(col("order_month").cast("date")) - month(col("cohort_month").cast("date"))))
    )
    return df


@dp.materialized_view(
    name=f"{GOLD}.agg_customer_lifetime",
    comment="Aggregate: CLV / RFM distribution per segment"
)
def agg_customer_lifetime():
    df = (
        spark.read.table(f"{GOLD}.dim_customer")
        .groupBy("customer_segment", "rfm_segment")
        .agg(
            count("customer_id").alias("customers"),
            _round(avg("total_orders"), 2).alias("avg_orders"),
            _round(avg("total_spent"), 2).alias("avg_spend"),
            _round(avg("customer_lifetime_value"), 2).alias("avg_clv"),
            _round(_sum("customer_lifetime_value"), 2).alias("total_clv"),
            _round(avg("recency_days"), 1).alias("avg_recency_days"),
        )
    )
    return df


@dp.materialized_view(
    name=f"{GOLD}.agg_item_performance",
    comment="Aggregate: units, revenue, margin per menu item per restaurant"
)
def agg_item_performance():
    df = (
        spark.read.table(f"{GOLD}.fct_order_items")
        .groupBy("restaurant_id", "item_id", "category", "sub_category")
        .agg(
            _sum("quantity").alias("units_sold"),
            count("order_detail_id").alias("times_ordered"),
            _round(_sum("line_revenue"), 2).alias("revenue"),
            _round(_sum("line_margin"), 2).alias("margin"),
            _round(avg("item_price_at_time"), 2).alias("avg_price"),
        )
        .withColumn("margin_pct",
            when(col("revenue") > 0,
                 _round(col("margin") / col("revenue") * 100, 2))
            .otherwise(lit(0.0)))
    )
    return df


@dp.materialized_view(
    name=f"{GOLD}.agg_menu_engineering",
    comment="Aggregate: menu engineering matrix (Stars / Puzzles / Plowhorses / Dogs)"
)
def agg_menu_engineering():
    perf = (
        spark.read.table(f"{GOLD}.agg_item_performance")
        .join(
            spark.read.table(f"{GOLD}.dim_menu_item")
                .select("item_id", "restaurant_id", "item_name", "price", "cost_to_make",
                        "gross_margin_gbp", "gross_margin_pct"),
            on=["item_id", "restaurant_id"],
            how="left",
        )
    )

    med = (
        perf.groupBy("restaurant_id")
        .agg(
            percentile_approx("units_sold", 0.5).alias("med_units"),
            percentile_approx("margin_pct", 0.5).alias("med_margin_pct"),
        )
    )

    df = (
        perf.join(med, on="restaurant_id", how="left")
        .withColumn("popularity", when(col("units_sold") >= col("med_units"), "High").otherwise("Low"))
        .withColumn("profitability", when(col("margin_pct") >= col("med_margin_pct"), "High").otherwise("Low"))
        .withColumn("classification",
            when((col("popularity") == "High") & (col("profitability") == "High"), "Star")
            .when((col("popularity") == "Low")  & (col("profitability") == "High"), "Puzzle")
            .when((col("popularity") == "High") & (col("profitability") == "Low"),  "Plowhorse")
            .otherwise("Dog"))
    )
    return df


@dp.materialized_view(
    name=f"{GOLD}.agg_ops_costs",
    comment="Aggregate: monthly operating costs per restaurant"
)
def agg_ops_costs():
    df = (
        spark.read.table(f"{GOLD}.fct_daily_ops")
        .groupBy("restaurant_id", "year_month")
        .agg(
            _round(_sum("electricity_cost"), 2).alias("electricity"),
            _round(_sum("water_cost"), 2).alias("water"),
            _round(_sum("gas_cost"), 2).alias("gas"),
            _round(_sum("maintenance_cost"), 2).alias("maintenance"),
            _round(_sum("other_bills_cost"), 2).alias("other"),
            _round(_sum("total_operating_cost"), 2).alias("total_cost"),
            _round(_sum("daily_revenue"), 2).alias("revenue"),
            _round(avg("average_wait_time_mins"), 1).alias("avg_wait_time_mins"),
            _round(avg("staff_on_shift"), 1).alias("avg_staff_on_shift"),
        )
        .withColumn("cost_to_revenue_pct",
            when(col("revenue") > 0,
                 _round(col("total_cost") / col("revenue") * 100, 2))
            .otherwise(lit(0.0)))
    )
    return df


@dp.materialized_view(
    name=f"{GOLD}.agg_staffing_efficiency",
    comment="Aggregate: staffing KPIs per restaurant and position"
)
def agg_staffing_efficiency():
    df = (
        spark.read.table(f"{GOLD}.dim_employee")
        .groupBy("restaurant_id", "position")
        .agg(
            count("employee_id").alias("headcount"),
            _sum(when(col("is_active"), 1).otherwise(0)).alias("active_headcount"),
            _sum(when(col("is_active") == False, 1).otherwise(0)).alias("left_headcount"),
            _round(avg("salary_per_hour"), 2).alias("avg_hourly_rate"),
            _round(avg("hours_per_week"), 1).alias("avg_hours_per_week"),
            _round(avg("performance_rating"), 2).alias("avg_rating"),
            _round(avg("tenure_months"), 1).alias("avg_tenure_months"),
            _round(_sum("weekly_cost_gbp"), 2).alias("weekly_payroll"),
        )
        .withColumn("turnover_pct",
            when(col("headcount") > 0,
                 _round(col("left_headcount") / col("headcount") * 100, 2))
            .otherwise(lit(0.0)))
    )
    return df


@dp.materialized_view(
    name=f"{GOLD}.agg_delivery_by_condition",
    comment="Aggregate: on-time % and avg duration by traffic and weather"
)
def agg_delivery_by_condition():
    df = (
        spark.read.table(f"{GOLD}.fct_delivery")
        .groupBy("traffic_condition", "weather_condition")
        .agg(
            count("delivery_id").alias("deliveries"),
            _round(avg("delivery_minutes"), 2).alias("avg_delivery_mins"),
            _round(avg("distance_miles"), 2).alias("avg_distance_miles"),
            _sum(when(col("is_on_time"), 1).otherwise(0)).alias("on_time_count"),
            _sum(when(col("is_delayed"), 1).otherwise(0)).alias("delayed_count"),
            _sum(when(col("is_cancelled"), 1).otherwise(0)).alias("cancelled_count"),
        )
        .withColumn("on_time_pct",
            when(col("deliveries") > 0,
                 _round(col("on_time_count") / col("deliveries") * 100, 2))
            .otherwise(lit(0.