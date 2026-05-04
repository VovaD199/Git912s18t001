# import yaml

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    countDistinct,
    round as spark_round,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)

from src.utils.config import load_config
from src.utils.validation import (
    validate_required_columns,
    validate_output_not_empty,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


USERS_SCHEMA = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("country", StringType(), True),
    StructField("registration_date", StringType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("order_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
])


def create_spark_session(app_name: str = "daily_revenue_by_country") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    logger.info("Reading CSV file: %s", path)

    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(path)
    )


def transform(users_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    logger.info("Validating input schemas")

    validate_required_columns(
        users_df,
        ["user_id", "country"],
        "users",
    )

    validate_required_columns(
        orders_df,
        ["order_id", "user_id", "order_date", "amount", "status"],
        "orders",
    )

    logger.info("Filtering completed orders")

    completed_orders_df = orders_df.filter(col("status") == "completed")

    logger.info("Joining orders with users")

    joined_df = completed_orders_df.join(
        users_df,
        on="user_id",
        how="inner",
    )

    logger.info("Calculating daily revenue by country")

    result_df = (
        joined_df
        .groupBy("order_date", "country")
        .agg(
            spark_sum("amount").alias("total_revenue"),
            count("order_id").alias("completed_orders_count"),
            countDistinct("user_id").alias("unique_customers_count"),
        )
        .withColumn(
            "avg_order_value",
            spark_round(
                col("total_revenue") / col("completed_orders_count"),
                2,
            ),
        )
        .select(
            "order_date",
            "country",
            "total_revenue",
            "completed_orders_count",
            "unique_customers_count",
            "avg_order_value",
        )
    )

    return result_df


def write_parquet(df: DataFrame, output_path: str) -> None:
    logger.info("Writing result to Parquet: %s", output_path)

    (
        df.write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet(output_path)
    )


def run_pipeline(
    users_path: str,
    orders_path: str,
    output_path: str,
) -> None:
    logger.info("Starting daily revenue pipeline")

    spark = create_spark_session()

    try:
        users_df = read_csv(spark, users_path, USERS_SCHEMA)
        orders_df = read_csv(spark, orders_path, ORDERS_SCHEMA)

        logger.info("Running transformation")
        result_df = transform(users_df, orders_df)

        logger.info("Validating output data")
        validate_output_not_empty(result_df)

        logger.info("Writing output")
        write_parquet(result_df, output_path)

        logger.info("Pipeline finished successfully")

    except Exception as e:
        logger.exception("Pipeline failed with error: %s", e)
        raise

    finally:
        spark.stop()


def main() -> None:
    config = load_config()

    run_pipeline(
        users_path=config["paths"]["users"],
        orders_path=config["paths"]["orders"],
        output_path=config["paths"]["output"],
    )


if __name__ == "__main__":
    main()