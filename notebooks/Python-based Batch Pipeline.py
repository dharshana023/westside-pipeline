# Databricks notebook source
# ============================================================
# API BATCH DATA PIPELINE
# Databricks Serverless Compatible
#
# API      -> Bronze -> Silver -> Gold
# ============================================================

import logging
from datetime import datetime, timezone

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ============================================================
# 1. LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

log = logging.getLogger("api_batch_pipeline")


# ============================================================
# 2. CONFIGURATION
# ============================================================

API_URL = "https://jsonplaceholder.typicode.com/posts"

BRONZE_TABLE = "bronze_api_posts"
SILVER_TABLE = "silver_api_posts"
GOLD_TABLE = "gold_api_post_summary"


# ============================================================
# 3. USE DATABRICKS-PROVIDED SPARK SESSION
# ============================================================

try:
    spark  # noqa: B018

    log.info(
        "Using the Databricks-provided SparkSession."
    )

except NameError:
    spark = (
        SparkSession.builder
        .appName("API-Batch-Pipeline")
        .getOrCreate()
    )

    log.info(
        "Created a new SparkSession."
    )


# IMPORTANT:
# Databricks Serverless does NOT support direct SparkContext access.
#
# DO NOT USE:
#
# spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 4. EXTRACT DATA FROM API
# ============================================================

log.info("Calling API...")

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

try:
    response = requests.get(
        API_URL,
        headers=headers,
        timeout=30,
    )

    log.info(
        "API HTTP status: %s",
        response.status_code,
    )

    response.raise_for_status()

    api_data = response.json()

    log.info(
        "API call successful."
    )

    log.info(
        "Number of records received: %d",
        len(api_data),
    )

except requests.exceptions.RequestException as e:
    log.error(
        "API request failed: %s",
        e,
    )

    raise


# ============================================================
# 5. CREATE INGESTION TIMESTAMP
# ============================================================

ingestion_timestamp = datetime.now(timezone.utc)


# ============================================================
# 6. CONVERT API RESPONSE INTO RECORDS
# ============================================================

records = []

for item in api_data:
    records.append(
        {
            "user_id": item.get("userId"),
            "post_id": item.get("id"),
            "title": item.get("title"),
            "body": item.get("body"),
            "ingestion_timestamp": ingestion_timestamp,
        }
    )


# ============================================================
# 7. DEFINE SPARK SCHEMA
# ============================================================

schema = T.StructType(
    [
        T.StructField(
            "user_id",
            T.IntegerType(),
            True,
        ),
        T.StructField(
            "post_id",
            T.IntegerType(),
            True,
        ),
        T.StructField(
            "title",
            T.StringType(),
            True,
        ),
        T.StructField(
            "body",
            T.StringType(),
            True,
        ),
        T.StructField(
            "ingestion_timestamp",
            T.TimestampType(),
            True,
        ),
    ]
)


# ============================================================
# 8. CREATE BRONZE DATAFRAME
# ============================================================

bronze_df = spark.createDataFrame(
    records,
    schema=schema,
)

print("==========================================")
print("BRONZE DATA")
print("==========================================")

bronze_df.show(
    10,
    truncate=False,
)

print(
    "Bronze record count:",
    bronze_df.count(),
)


# ============================================================
# 9. WRITE BRONZE TABLE
# ============================================================

log.info(
    "Writing Bronze table: %s",
    BRONZE_TABLE,
)

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(BRONZE_TABLE)
)

log.info(
    "Bronze table created successfully."
)


# ============================================================
# 10. SILVER - DATA CLEANING
# ============================================================

silver_df = (
    bronze_df
    .dropDuplicates(["post_id"])
    .filter(
        F.col("post_id").isNotNull()
    )
    .filter(
        F.col("user_id").isNotNull()
    )
    .filter(
        F.col("title").isNotNull()
    )
    .withColumn(
        "title",
        F.trim(F.col("title")),
    )
    .withColumn(
        "body",
        F.trim(F.col("body")),
    )
)


print("==========================================")
print("SILVER DATA")
print("==========================================")

silver_df.show(
    10,
    truncate=False,
)

print(
    "Silver record count:",
    silver_df.count(),
)


# ============================================================
# 11. WRITE SILVER TABLE
# ============================================================

log.info(
    "Writing Silver table: %s",
    SILVER_TABLE,
)

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_TABLE)
)

log.info(
    "Silver table created successfully."
)


# ============================================================
# 12. GOLD - BUSINESS SUMMARY
# ============================================================

gold_df = (
    silver_df
    .groupBy("user_id")
    .agg(
        F.count("post_id")
        .alias("total_posts"),

        F.countDistinct("post_id")
        .alias("unique_posts"),

        F.avg(
            F.length("title")
        )
        .alias("average_title_length"),

        F.avg(
            F.length("body")
        )
        .alias("average_body_length"),
    )
    .withColumn(
        "average_title_length",
        F.round(
            F.col("average_title_length"),
            2,
        ),
    )
    .withColumn(
        "average_body_length",
        F.round(
            F.col("average_body_length"),
            2,
        ),
    )
    .orderBy("user_id")
)


print("==========================================")
print("GOLD DATA")
print("==========================================")

gold_df.show(
    truncate=False,
)


# ============================================================
# 13. WRITE GOLD TABLE
# ============================================================

log.info(
    "Writing Gold table: %s",
    GOLD_TABLE,
)

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_TABLE)
)

log.info(
    "Gold table created successfully."
)


# ============================================================
# 14. VERIFY BRONZE TABLE
# ============================================================

print("==========================================")
print("VERIFY BRONZE TABLE")
print("==========================================")

spark.sql(
    f"""
    SELECT *
    FROM {BRONZE_TABLE}
    LIMIT 10
    """
).show(
    truncate=False,
)


# ============================================================
# 15. VERIFY SILVER TABLE
# ============================================================

print("==========================================")
print("VERIFY SILVER TABLE")
print("==========================================")

spark.sql(
    f"""
    SELECT *
    FROM {SILVER_TABLE}
    LIMIT 10
    """
).show(
    truncate=False,
)


# ============================================================
# 16. VERIFY GOLD TABLE
# ============================================================

print("==========================================")
print("VERIFY GOLD TABLE")
print("==========================================")

spark.sql(
    f"""
    SELECT *
    FROM {GOLD_TABLE}
    ORDER BY user_id
    """
).show(
    truncate=False,
)


# ============================================================
# 17. TABLE COUNTS
# ============================================================

print("==========================================")
print("TABLE COUNTS")
print("==========================================")

print(
    "Bronze:",
    spark.table(BRONZE_TABLE).count(),
)

print(
    "Silver:",
    spark.table(SILVER_TABLE).count(),
)

print(
    "Gold:",
    spark.table(GOLD_TABLE).count(),
)


# ============================================================
# 18. FINAL MESSAGE
# ============================================================

print()
print("==========================================")
print("API BATCH PIPELINE COMPLETED SUCCESSFULLY")
print("==========================================")

print("API:", API_URL)
print("Bronze Table:", BRONZE_TABLE)
print("Silver Table:", SILVER_TABLE)
print("Gold Table:", GOLD_TABLE)

print("==========================================")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Extract — pull raw data from the REST API
# MAGIC
# MAGIC A real batch job should never hard-fail just because a downstream API had a blip, and it
# MAGIC shouldn't be untestable when there's no network — so `extract()` retries briefly, then
# MAGIC falls back to a small embedded sample of the same shape if the API truly can't be
# MAGIC reached, and always logs which path was taken.

# COMMAND ----------

# ============================================================
# API BATCH PIPELINE
# Databricks Serverless Compatible
# API -> Bronze -> Silver -> Gold
# ============================================================

import logging
from datetime import datetime, timezone

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ============================================================
# 1. LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

log = logging.getLogger("batch_pipeline")


# ============================================================
# 2. CONFIGURATION
# ============================================================

API_URL = "https://fakestoreapi.com/products"

BRONZE_TABLE = "bronze_products"
SILVER_TABLE = "silver_products"
GOLD_TABLE = "gold_product_summary"


# ============================================================
# 3. DATABRICKS SPARK SESSION
# ============================================================

try:
    spark  # noqa: B018
    log.info("Using the Databricks-provided SparkSession.")

except NameError:
    spark = (
        SparkSession.builder
        .appName("api-batch-pipeline")
        .getOrCreate()
    )


# IMPORTANT:
# Do NOT use:
# spark.sparkContext.setLogLevel("ERROR")
#
# SparkContext is not supported on Databricks Serverless.


# ============================================================
# 4. FALLBACK SAMPLE DATA
# ============================================================

_SAMPLE_FALLBACK = [
    {
        "id": 1,
        "title": "Fjallraven Foldsack No. 1 Backpack",
        "price": 109.95,
        "description": "Fits 15 laptops, roomy main compartment.",
        "category": "men's clothing",
        "image": "https://fakestoreapi.com/img/1.jpg",
        "rating": {
            "rate": 3.9,
            "count": 120,
        },
    },
    {
        "id": 2,
        "title": "Mens Casual Slim Fit T-Shirt",
        "price": 22.3,
        "description": "Slim-fitting style, contrast raglan long sleeve.",
        "category": "men's clothing",
        "image": "https://fakestoreapi.com/img/2.jpg",
        "rating": {
            "rate": 4.1,
            "count": 259,
        },
    },
    {
        "id": 3,
        "title": "WD 2TB External Hard Drive",
        "price": 64.0,
        "description": "USB 3.0 and USB 2.0 compatibility.",
        "category": "electronics",
        "image": "https://fakestoreapi.com/img/3.jpg",
        "rating": {
            "rate": 3.3,
            "count": 203,
        },
    },
    {
        "id": 4,
        "title": "White Gold Plated Princess Ring",
        "price": 9.99,
        "description": "Classic princess-cut solitaire ring.",
        "category": "jewelery",
        "image": "https://fakestoreapi.com/img/4.jpg",
        "rating": {
            "rate": 2.1,
            "count": 62,
        },
    },
    {
        "id": 5,
        "title": "Women's Boyfriend Denim Jacket",
        "price": 56.99,
        "description": "100% cotton, casual boyfriend fit.",
        "category": "women's clothing",
        "image": "https://fakestoreapi.com/img/5.jpg",
        "rating": {
            "rate": 4.5,
            "count": 41,
        },
    },
]


# ============================================================
# 5. EXTRACT FUNCTION
# ============================================================

def extract(
    api_url: str = API_URL,
    timeout_s: int = 8,
    retries: int = 2,
) -> list[dict]:
    """
    Extract raw product records from REST API.

    If the API is unavailable or returns an error such as 403,
    the embedded sample dataset is used.
    """

    for attempt in range(1, retries + 1):
        try:
            log.info(
                f"Extract attempt {attempt}/{retries}: "
                f"GET {api_url}"
            )

            resp = requests.get(
                api_url,
                timeout=timeout_s,
            )

            log.info(
                f"HTTP status: {resp.status_code}"
            )

            resp.raise_for_status()

            records = resp.json()

            log.info(
                f"Extracted {len(records)} records from the API."
            )

            return records

        except Exception as e:  # noqa: BLE001
            log.warning(
                f"Extract attempt {attempt} failed: "
                f"{e.__class__.__name__}: {e}"
            )

    log.warning(
        "All API attempts failed; "
        "falling back to the embedded sample dataset."
    )

    return _SAMPLE_FALLBACK


# ============================================================
# 6. RUN EXTRACTION
# ============================================================

raw_records = extract()

print("==========================================")
print("RAW DATA")
print("==========================================")

print(
    f"Raw record count: {len(raw_records)}"
)

print()

for record in raw_records:
    print(record)


# ============================================================
# 7. TRANSFORM JSON INTO FLAT RECORDS
# ============================================================

ingestion_timestamp = datetime.now(timezone.utc)

records = []

for product in raw_records:
    rating = product.get("rating", {})

    records.append(
        {
            "product_id": product.get("id"),
            "title": product.get("title"),
            "price": product.get("price"),
            "description": product.get("description"),
            "category": product.get("category"),
            "image": product.get("image"),
            "rating_rate": rating.get("rate"),
            "rating_count": rating.get("count"),
            "ingestion_timestamp": ingestion_timestamp,
        }
    )


# ============================================================
# 8. SPARK SCHEMA
# ============================================================

schema = T.StructType(
    [
        T.StructField(
            "product_id",
            T.IntegerType(),
            True,
        ),
        T.StructField(
            "title",
            T.StringType(),
            True,
        ),
        T.StructField(
            "price",
            T.DoubleType(),
            True,
        ),
        T.StructField(
            "description",
            T.StringType(),
            True,
        ),
        T.StructField(
            "category",
            T.StringType(),
            True,
        ),
        T.StructField(
            "image",
            T.StringType(),
            True,
        ),
        T.StructField(
            "rating_rate",
            T.DoubleType(),
            True,
        ),
        T.StructField(
            "rating_count",
            T.IntegerType(),
            True,
        ),
        T.StructField(
            "ingestion_timestamp",
            T.TimestampType(),
            True,
        ),
    ]
)


# ============================================================
# 9. BRONZE DATAFRAME
# ============================================================

bronze_df = spark.createDataFrame(
    records,
    schema=schema,
)

print()
print("==========================================")
print("BRONZE DATA")
print("==========================================")

bronze_df.show(
    truncate=False,
)

print(
    "Bronze record count:",
    bronze_df.count(),
)


# ============================================================
# 10. WRITE BRONZE TABLE
# ============================================================

log.info(
    f"Writing Bronze table: {BRONZE_TABLE}"
)

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(BRONZE_TABLE)
)

log.info(
    "Bronze table written successfully."
)


# ============================================================
# 11. SILVER - CLEAN DATA
# ============================================================

silver_df = (
    bronze_df
    .dropDuplicates(["product_id"])
    .filter(
        F.col("product_id").isNotNull()
    )
    .filter(
        F.col("title").isNotNull()
    )
    .filter(
        F.col("price").isNotNull()
    )
    .filter(
        F.col("price") >= 0
    )
    .withColumn(
        "title",
        F.trim(F.col("title")),
    )
    .withColumn(
        "category",
        F.trim(F.col("category")),
    )
    .withColumn(
        "description",
        F.trim(F.col("description")),
    )
)


# ============================================================
# 12. SHOW SILVER DATA
# ============================================================

print()
print("==========================================")
print("SILVER DATA")
print("==========================================")

silver_df.show(
    truncate=False,
)

print(
    "Silver record count:",
    silver_df.count(),
)


# ============================================================
# 13. WRITE SILVER TABLE
# ============================================================

log.info(
    f"Writing Silver table: {SILVER_TABLE}"
)

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_TABLE)
)

log.info(
    "Silver table written successfully."
)


# ============================================================
# 14. GOLD - PRODUCT CATEGORY SUMMARY
# ============================================================

gold_df = (
    silver_df
    .groupBy("category")
    .agg(
        F.count("product_id")
        .alias("product_count"),

        F.round(
            F.avg("price"),
            2,
        ).alias("average_price"),

        F.round(
            F.min("price"),
            2,
        ).alias("minimum_price"),

        F.round(
            F.max("price"),
            2,
        ).alias("maximum_price"),

        F.round(
            F.sum("price"),
            2,
        ).alias("total_price"),

        F.round(
            F.avg("rating_rate"),
            2,
        ).alias("average_rating"),

        F.sum("rating_count")
        .alias("total_rating_count"),
    )
    .orderBy("category")
)


# ============================================================
# 15. SHOW GOLD DATA
# ============================================================

print()
print("==========================================")
print("GOLD DATA")
print("==========================================")

gold_df.show(
    truncate=False,
)


# ============================================================
# 16. WRITE GOLD TABLE
# ============================================================

log.info(
    f"Writing Gold table: {GOLD_TABLE}"
)

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_TABLE)
)

log.info(
    "Gold table written successfully."
)


# ============================================================
# 17. VERIFY BRONZE TABLE
# ============================================================

print()
print("==========================================")
print("VERIFY BRONZE TABLE")
print("==========================================")

spark.sql(
    f"""
    SELECT *
    FROM {BRONZE_TABLE}
    """
).show(
    truncate=False,
)


# ============================================================
# 18. VERIFY SILVER TABLE
# ============================================================

print()
print("==========================================")
print("VERIFY SILVER TABLE")
print("==========================================")

spark.sql(
    f"""
    SELECT *
    FROM {SILVER_TABLE}
    """
).show(
    truncate=False,
)


# ============================================================
# 19. VERIFY GOLD TABLE
# ============================================================

print()
print("==========================================")
print("VERIFY GOLD TABLE")
print("==========================================")

spark.sql(
    f"""
    SELECT *
    FROM {GOLD_TABLE}
    ORDER BY category
    """
).show(
    truncate=False,
)


# ============================================================
# 20. TABLE COUNTS
# ============================================================

print()
print("==========================================")
print("TABLE COUNTS")
print("==========================================")

bronze_count = spark.table(BRONZE_TABLE).count()
silver_count = spark.table(SILVER_TABLE).count()
gold_count = spark.table(GOLD_TABLE).count()

print(
    f"Bronze records : {bronze_count}"
)

print(
    f"Silver records : {silver_count}"
)

print(
    f"Gold records   : {gold_count}"
)


# ============================================================
# 21. FINAL STATUS
# ============================================================

print()
print("==========================================")
print("PIPELINE COMPLETED SUCCESSFULLY")
print("==========================================")

print(
    "Source      : Fake Store API / Fallback Dataset"
)

print(
    f"Bronze Table: {BRONZE_TABLE}"
)

print(
    f"Silver Table: {SILVER_TABLE}"
)

print(
    f"Gold Table  : {GOLD_TABLE}"
)

print("==========================================")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transform — clean, standardize, enrich
# MAGIC
# MAGIC `transform()` takes the raw list of dicts and returns a clean Spark DataFrame:
# MAGIC
# MAGIC 1. **Flatten** the nested `rating` object into `rating_rate` / `rating_count`
# MAGIC 2. **Cleanse** — trim/lowercase category text, cast price to a proper numeric type,
# MAGIC    drop duplicate product IDs
# MAGIC 3. **Enrich** — convert `price` (USD) to INR using a live FX-rate API call (with an
# MAGIC    offline fallback, same pattern as extraction)
# MAGIC 4. **Add ingestion metadata** — `ingested_at` timestamp and `source_system`, which any
# MAGIC    real warehouse table needs for lineage/auditing

# COMMAND ----------

# ============================================================
# API BATCH PIPELINE
# EXTRACT + FX ENRICHMENT + TRANSFORM
# Databricks Serverless Compatible
# ============================================================

import logging
from datetime import datetime, timezone

import pandas as pd
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ============================================================
# 1. LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

log = logging.getLogger("batch_pipeline")


# ============================================================
# 2. CONFIGURATION
# ============================================================

API_URL = "https://fakestoreapi.com/products"


# ============================================================
# 3. DATABRICKS SPARK SESSION
# ============================================================

try:
    spark  # noqa: B018

    log.info(
        "Using the Databricks-provided SparkSession."
    )

except NameError:
    spark = (
        SparkSession.builder
        .appName("api-batch-pipeline")
        .getOrCreate()
    )

    log.info(
        "Created SparkSession."
    )


# IMPORTANT:
# Do NOT use:
#
# spark.sparkContext.setLogLevel("ERROR")
#
# SparkContext is not supported on Databricks Serverless.


# ============================================================
# 4. FALLBACK PRODUCT DATA
# ============================================================

_SAMPLE_FALLBACK = [
    {
        "id": 1,
        "title": "Fjallraven Foldsack No. 1 Backpack",
        "price": 109.95,
        "description": "Fits 15 laptops, roomy main compartment.",
        "category": "men's clothing",
        "image": "https://fakestoreapi.com/img/1.jpg",
        "rating": {
            "rate": 3.9,
            "count": 120,
        },
    },
    {
        "id": 2,
        "title": "Mens Casual Slim Fit T-Shirt",
        "price": 22.3,
        "description": "Slim-fitting style, contrast raglan long sleeve.",
        "category": "men's clothing",
        "image": "https://fakestoreapi.com/img/2.jpg",
        "rating": {
            "rate": 4.1,
            "count": 259,
        },
    },
    {
        "id": 3,
        "title": "WD 2TB External Hard Drive",
        "price": 64.0,
        "description": "USB 3.0 and USB 2.0 compatibility.",
        "category": "electronics",
        "image": "https://fakestoreapi.com/img/3.jpg",
        "rating": {
            "rate": 3.3,
            "count": 203,
        },
    },
    {
        "id": 4,
        "title": "White Gold Plated Princess Ring",
        "price": 9.99,
        "description": "Classic princess-cut solitaire ring.",
        "category": "jewelery",
        "image": "https://fakestoreapi.com/img/4.jpg",
        "rating": {
            "rate": 2.1,
            "count": 62,
        },
    },
    {
        "id": 5,
        "title": "Women's Boyfriend Denim Jacket",
        "price": 56.99,
        "description": "100% cotton, casual boyfriend fit.",
        "category": "women's clothing",
        "image": "https://fakestoreapi.com/img/5.jpg",
        "rating": {
            "rate": 4.5,
            "count": 41,
        },
    },
]


# ============================================================
# 5. EXTRACT PRODUCTS
# ============================================================

def extract(
    api_url: str = API_URL,
    timeout_s: int = 8,
    retries: int = 2,
) -> list[dict]:
    """
    Extract product records from Fake Store API.

    If the API is unavailable or returns 403,
    the embedded sample dataset is used.
    """

    for attempt in range(1, retries + 1):
        try:
            log.info(
                f"Extract attempt {attempt}/{retries}: "
                f"GET {api_url}"
            )

            resp = requests.get(
                api_url,
                timeout=timeout_s,
            )

            log.info(
                f"API HTTP status: {resp.status_code}"
            )

            resp.raise_for_status()

            records = resp.json()

            log.info(
                f"Extracted {len(records)} records from API."
            )

            return records

        except Exception as e:  # noqa: BLE001
            log.warning(
                f"Extract attempt {attempt} failed: "
                f"{e.__class__.__name__}: {e}"
            )

    log.warning(
        "All API attempts failed; "
        "using embedded fallback dataset."
    )

    return _SAMPLE_FALLBACK


# ============================================================
# 6. RUN PRODUCT EXTRACTION
# ============================================================

raw_records = extract()

print("==========================================")
print("RAW RECORDS")
print("==========================================")

print(
    "Raw record count:",
    len(raw_records),
)

print()

for record in raw_records:
    print(record)


# ============================================================
# 7. GET USD TO INR EXCHANGE RATE
# ============================================================

def get_usd_to_inr_rate() -> float:
    """
    Get USD to INR exchange rate from external API.

    If API is unavailable, use fallback rate.
    """

    try:
        fx_url = (
            "https://api.exchangerate-api.com/v4/latest/USD"
        )

        log.info(
            "Calling FX API..."
        )

        resp = requests.get(
            fx_url,
            timeout=5,
        )

        log.info(
            f"FX API HTTP status: {resp.status_code}"
        )

        resp.raise_for_status()

        rate = resp.json()["rates"]["INR"]

        log.info(
            f"Live FX rate extracted: "
            f"1 USD = {rate} INR"
        )

        return float(rate)

    except Exception as e:  # noqa: BLE001
        fallback_rate = 83.0

        log.warning(
            f"FX API unavailable "
            f"({e.__class__.__name__}); "
            f"using fallback rate: {fallback_rate}"
        )

        return fallback_rate


# ============================================================
# 8. TRANSFORM
# ============================================================

def transform(records: list[dict]):
    """
    Clean, standardize and enrich raw product records.

    Returns a Spark DataFrame.
    """

    # --------------------------------------------------------
    # Flatten nested JSON
    # --------------------------------------------------------

    pdf = pd.json_normalize(records)

    pdf = pdf.rename(
        columns={
            "rating.rate": "rating_rate",
            "rating.count": "rating_count",
        }
    )


    # --------------------------------------------------------
    # Create Spark DataFrame
    # --------------------------------------------------------

    df = spark.createDataFrame(pdf)


    # --------------------------------------------------------
    # Get USD -> INR rate
    # --------------------------------------------------------

    usd_to_inr = get_usd_to_inr_rate()

    print()
    print(
        f"USD to INR rate used: {usd_to_inr}"
    )


    # --------------------------------------------------------
    # Transform columns
    # --------------------------------------------------------

    df = (
        df
        .dropDuplicates(["id"])
        .withColumn(
            "category",
            F.lower(
                F.trim(
                    F.col("category")
                )
            ),
        )
        .withColumn(
            "title",
            F.trim(
                F.col("title")
            ),
        )
        .withColumn(
            "price_usd",
            F.col("price").cast(
                T.DoubleType()
            ),
        )
        .withColumn(
            "price_inr",
            F.round(
                F.col("price_usd")
                * F.lit(usd_to_inr),
                2,
            ),
        )
        .withColumn(
            "rating_rate",
            F.col("rating_rate").cast(
                T.DoubleType()
            ),
        )
        .withColumn(
            "rating_count",
            F.col("rating_count").cast(
                T.IntegerType()
            ),
        )
        .withColumn(
            "ingested_at",
            F.lit(
                datetime.now(timezone.utc).isoformat()
            ),
        )
        .withColumn(
            "source_system",
            F.lit(
                "fakestoreapi.com/products"
            ),
        )
        .drop("price")
        .select(
            "id",
            "title",
            "category",
            "price_usd",
            "price_inr",
            "rating_rate",
            "rating_count",
            "description",
            "image",
            "ingested_at",
            "source_system",
        )
    )


    # --------------------------------------------------------
    # Data Quality Check
    # --------------------------------------------------------

    before = df.count()

    df = df.dropna(
        subset=[
            "id",
            "price_usd",
        ]
    )

    after = df.count()

    dropped = before - after

    if dropped:
        log.warning(
            f"Dropped {dropped} row(s) "
            f"with null id/price."
        )
    else:
        log.info(
            "Data quality check passed. "
            "No rows dropped."
        )

    return df


# ============================================================
# 9. RUN TRANSFORMATION
# ============================================================

transformed_df = transform(
    raw_records
)


# ============================================================
# 10. DISPLAY TRANSFORMED DATA
# ============================================================

print()
print("==========================================")
print("TRANSFORMED DATA")
print("==========================================")

transformed_df.show(
    5,
    truncate=40,
)


# ============================================================
# 11. PRINT SCHEMA
# ============================================================

print()
print("==========================================")
print("TRANSFORMED SCHEMA")
print("==========================================")

transformed_df.printSchema()


# ============================================================
# 12. DISPLAY RECORD COUNT
# ============================================================

print()
print("==========================================")
print("TRANSFORMED RECORD COUNT")
print("==========================================")

print(
    "Records:",
    transformed_df.count(),
)


# ============================================================
# 13. DISPLAY PRODUCT PRICE CONVERSION
# ============================================================

print()
print("==========================================")
print("USD TO INR CONVERSION")
print("==========================================")

transformed_df.select(
    "id",
    "title",
    "price_usd",
    "price_inr",
).show(
    truncate=False,
)


# ============================================================
# 14. DISPLAY CATEGORY SUMMARY
# ============================================================

print()
print("==========================================")
print("CATEGORY SUMMARY")
print("==========================================")

(
    transformed_df
    .groupBy("category")
    .agg(
        F.count("id").alias("product_count"),
        F.round(
            F.avg("price_usd"),
            2,
        ).alias("avg_price_usd"),
        F.round(
            F.avg("price_inr"),
            2,
        ).alias("avg_price_inr"),
        F.round(
            F.avg("rating_rate"),
            2,
        ).alias("avg_rating"),
    )
    .orderBy("category")
    .show(
        truncate=False,
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load — write into the target warehouse table
# MAGIC
# MAGIC `load()` writes the transformed DataFrame into a Delta table (Parquet fallback locally),
# MAGIC using a full overwrite.

# COMMAND ----------

# ============================================================
# LOAD - WRITE TO MANAGED DELTA TABLE
# Databricks Serverless Compatible
# ============================================================

TARGET_TABLE = "dim_products_warehouse"


def load(
    df,
    table_name: str = TARGET_TABLE,
    mode: str = "overwrite",
) -> int:
    """
    Load the transformed DataFrame into a managed
    Delta warehouse table.

    Databricks Serverless compatible.
    Does not use DBFS paths.
    """

    log.info(
        f"Loading data into table '{table_name}'..."
    )

    (
        df.write
        .format("delta")
        .mode(mode)
        .saveAsTable(table_name)
    )

    loaded = spark.table(table_name)

    row_count = loaded.count()

    log.info(
        f"Loaded {row_count} rows into '{table_name}' "
        f"(managed Delta table)."
    )

    return row_count


# ============================================================
# RUN LOAD
# ============================================================

rows_loaded = load(transformed_df)

print()
print("==========================================")
print("LOAD COMPLETED")
print("==========================================")

print(
    f"Rows loaded: {rows_loaded}"
)

print(
    f"Target table: {TARGET_TABLE}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Orchestration — tie it together as one batch job

# COMMAND ----------


def run_pipeline() -> None:
    log.info("=== Batch pipeline started ===")

    try:
        # 1. Extract
        records = extract()

        log.info(
            f"Extracted {len(records)} raw records."
        )

        # 2. Transform
        df = transform(records)

        log.info(
            "Transformation completed."
        )

        # 3. Load
        n = load(df)

        log.info(
            f"=== Batch pipeline finished successfully: "
            f"{n} rows in '{TARGET_TABLE}' ==="
        )

    except Exception:
        log.exception(
            "=== Batch pipeline FAILED ==="
        )
        raise


# Run the complete pipeline
run_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify the load

# COMMAND ----------

# Check the loaded data by category
spark.sql(
    f"""
    SELECT
        category,
        COUNT(*) AS n_products,
        ROUND(AVG(price_usd), 2) AS avg_price_usd
    FROM {TARGET_TABLE}
    GROUP BY category
    ORDER BY n_products DESC
    """
).show()


# Verify that the target table is not empty
assert spark.table(TARGET_TABLE).count() > 0, (
    "Load produced an empty table!"
)

print(
    "Load verified: target table is populated."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Bonus — incremental loading with MERGE INTO
# MAGIC
# MAGIC A full overwrite re-writes the whole table every run. Once the source is large or you
# MAGIC only want to apply changes, a batch job typically upserts by business key instead.

# COMMAND ----------

from delta.tables import DeltaTable

# Simulate a later API run:
# - Product 1 price is changed
# - Product 99 is a new product

incoming_records = raw_records.copy()

incoming_records[0] = {
    **incoming_records[0],
    "price": incoming_records[0]["price"] + 5.0,
}

incoming_records.append(
    {
        "id": 99,
        "title": "New Arrival Product",
        "price": 39.99,
        "description": "Freshly added in this run.",
        "category": "electronics",
        "image": "https://fakestoreapi.com/img/99.jpg",
        "rating": {
            "rate": 4.0,
            "count": 1,
        },
    }
)


# Transform incoming records
incoming_df = transform(
    incoming_records
)


# Access the managed Delta table
target = DeltaTable.forName(
    spark,
    TARGET_TABLE,
)


# Perform MERGE / UPSERT
(
    target.alias("t")
    .merge(
        incoming_df.alias("s"),
        "t.id = s.id",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)


# Check final row count
updated_count = spark.table(
    TARGET_TABLE
).count()

print(
    f"After incremental MERGE: {updated_count} rows "
    f"(expected {rows_loaded + 1})"
)


# Display updated product 1 and new product 99
(
    spark.table(TARGET_TABLE)
    .filter("id IN (1, 99)")
    .show(
        truncate=40
    )
)

