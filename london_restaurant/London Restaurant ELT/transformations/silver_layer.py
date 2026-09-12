from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, trim, percentile_approx


def cap_outliers(df, columns):
    """Cap outliers at 1.5*IQR bounds using cross join with aggregate stats."""
    if not columns:
        return df
    agg_exprs = []
    for c in columns:
        agg_exprs.extend([
            percentile_approx(col(c), 0.25).alias(f"_q1_{c}"),
            percentile_approx(col(c), 0.75).alias(f"_q3_{c}"),
        ])
    stats = df.agg(*agg_exprs)
    for c in columns:
        stats = stats.withColumn(f"_lower_{c}", col(f"_q1_{c}") - 1.5 * (col(f"_q3_{c}") - col(f"_q1_{c}")))
        stats = stats.withColumn(f"_upper_{c}", col(f"_q3_{c}") + 1.5 * (col(f"_q3_{c}") - col(f"_q1_{c}")))
    result = df.crossJoin(stats)
    for c in columns:
        result = result.withColumn(
            c,
            when(col(c) < col(f"_lower_{c}"), col(f"_lower_{c}"))
            .when(col(c) > col(f"_upper_{c}"), col(f"_upper_{c}"))
            .otherwise(col(c)),
        )
    drop_cols = [name for name in result.columns if name.startswith("_")]
    return result.drop(*drop_cols)


@dp.materialized_view(name="restaurant_dev.silver.restaurants", comment="Silver layer: cleaned restaurant data")
def silver_restaurants():
    return (
        spark.read.table("restaurant_dev.bronze.restaurants")
        .drop("_rescued_data")
        .withColumn("restaurant_name", trim(col("restaurant_name")))
        .withColumn("address", trim(col("address")))
        .withColumn("postcode", trim(col("postcode")))
        .withColumn("area", trim(col("area")))
        .withColumn("phone_number", trim(col("phone_number")))
    )


@dp.materialized_view(name="restaurant_dev.silver.customers", comment="Silver layer: cleaned customer data")
def silver_customers():
    df = (
        spark.read.table("restaurant_dev.bronze.customers")
        .drop("_rescued_data")
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("email", trim(col("email")))
        .withColumn("phone", trim(col("phone")))
        .withColumn("postcode", trim(col("postcode")))
        .fillna({"email": "Unknown", "phone": "Unknown"})
    )
    return cap_outliers(df, ["avg_order_value"])


@dp.materialized_view(name="restaurant_dev.silver.employees", comment="Silver layer: cleaned employee data")
def silver_employees():
    df = (
        spark.read.table("restaurant_dev.bronze.employees")
        .drop("_rescued_data")
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("position", trim(col("position")))
        .withColumn("reason_for_leaving", trim(col("reason_for_leaving")))
        .fillna({"performance_rating": 0.0, "reason_for_leaving": "N/A"})
    )
    return cap_outliers(df, ["salary_per_hour", "performance_rating"])


@dp.materialized_view(name="restaurant_dev.silver.menu_items", comment="Silver layer: cleaned menu item data")
def silver_menu_items():
    df = (
        spark.read.table("restaurant_dev.bronze.menu_items")
        .drop("_rescued_data")
        .withColumn("item_name", trim(col("item_name")))
        .withColumn("category", trim(col("category")))
        .withColumn("sub_category", trim(col("sub_category")))
        .fillna({"sub_category": "N/A"})
    )
    return cap_outliers(df, ["preparation_time_mins", "cost_to_make"])


@dp.materialized_view(name="restaurant_dev.silver.orders", comment="Silver layer: cleaned order data")
def silver_orders():
    df = (
        spark.read.table("restaurant_dev.bronze.orders")
        .drop("_rescued_data")
        .withColumn("customer_id", col("customer_id").cast("int"))
        .withColumn("customer_name", trim(col("customer_name")))
        .withColumn("delivery_postcode", trim(col("delivery_postcode")))
        .withColumn("order_type", trim(col("order_type")))
        .withColumn("payment_method", trim(col("payment_method")))
        .withColumn("order_status", trim(col("order_status")))
        .fillna({"customer_rating": 0.0, "customer_name": "N/A"})
    )
    return cap_outliers(df, ["total_amount", "delivery_time_mins", "delivery_distance_miles"])


@dp.materialized_view(name="restaurant_dev.silver.order_details", comment="Silver layer: cleaned order detail data")
def silver_order_details():
    df = (
        spark.read.table("restaurant_dev.bronze.order_details")
        .drop("_rescued_data")
        .withColumn("special_instructions", trim(col("special_instructions")))
    )
    return cap_outliers(df, ["item_price_at_time"])


@dp.materialized_view(name="restaurant_dev.silver.customer_reviews", comment="Silver layer: cleaned customer review data")
def silver_customer_reviews():
    df = (
        spark.read.table("restaurant_dev.bronze.customer_reviews")
        .drop("_rescued_data")
        .withColumn("customer_id", col("customer_id").cast("int"))
        .withColumn("review_text", trim(col("review_text")))
        .withColumn("category", trim(col("category")))
        .fillna({"review_text": "No review provided", "rating": 0.0, "sentiment_score": 0.0})
    )
    return cap_outliers(df, ["sentiment_score"])


@dp.materialized_view(name="restaurant_dev.silver.daily_operations", comment="Silver layer: cleaned daily operations data")
def silver_daily_operations():
    df = (
        spark.read.table("restaurant_dev.bronze.daily_operations")
        .drop("_rescued_data")
        .dropDuplicates(["restaurant_id", "date"])
    )
    return cap_outliers(df, [
        "total_customers_eat_in", "total_customers_delivery",
        "total_orders", "delivery_orders_count", "eat_in_orders_count",
    ])


@dp.materialized_view(name="restaurant_dev.silver.delivery_performance", comment="Silver layer: cleaned delivery performance data")
def silver_delivery_performance():
    df = (
        spark.read.table("restaurant_dev.bronze.delivery_performance")
        .drop("_rescued_data")
        .withColumn("traffic_condition", trim(col("traffic_condition")))
        .withColumn("weather_condition", trim(col("weather_condition")))
        .withColumn("delivery_status", trim(col("delivery_status")))
    )
    return cap_outliers(df, ["distance_miles"])


@dp.materialized_view(name="restaurant_dev.silver.inventory", comment="Silver layer: cleaned inventory data")
def silver_inventory():
    df = (
        spark.read.table("restaurant_dev.bronze.inventory")
        .drop("_rescued_data")
        .fillna({"waste_stock": 0.0, "waste_value": 0.0})
    )
    return cap_outliers(df, ["stock_value", "waste_value", "closing_stock"])