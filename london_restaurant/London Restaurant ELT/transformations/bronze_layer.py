from pyspark import pipelines as dp

BASE_PATH = "/Volumes/restaurant_dev/london_dev_schema/raw/restaurant_data"


@dp.table(name="restaurants", comment="Bronze layer: raw restaurant data from CSV files")
def restaurants():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{BASE_PATH}/restaurants/")
    )


@dp.table(name="customers", comment="Bronze layer: raw customer data from CSV files")
def customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{BASE_PATH}/customers/")
    )


@dp.table(name="employees", comment="Bronze layer: raw employee data from CSV files")
def employees():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{BASE_PATH}/employees/")
    )


@dp.table(name="menu_items", comment="Bronze layer: raw menu item data from CSV files")
def menu_items():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{BASE_PATH}/menu_items/")
    )


@dp.table(name="orders", comment="Bronze layer: raw order data from CSV files")
def orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{BASE_PATH}/orders/")
    )


@dp.table(name="order_details", comment="Bronze layer: raw order detail data from CSV files")
def order_details():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{BASE_PATH}/order_details/")
    )


@dp.table(name="customer_reviews", comment="Bronze layer: raw customer review data from CSV files")
def customer_reviews():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{BASE_PATH}/customer_reviews/")
    )


@dp.table(name="daily_operations", comment="Bronze layer: raw daily operations data from CSV files")
def daily_operations():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{BASE_PATH}/daily_operations/")
    )


@dp.table(name="delivery_performance", comment="Bronze layer: raw delivery performance data from CSV files")
def delivery_performance():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{BASE_PATH}/delivery_performance/")
    )


@dp.table(name="inventory", comment="Bronze layer: raw inventory data from CSV files")
def inventory():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{BASE_PATH}/inventory/")
    )