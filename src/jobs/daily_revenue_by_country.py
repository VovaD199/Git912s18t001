from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(users_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    """
    Transform raw users and orders data into daily revenue by country.

    Expected output columns:
    - order_date
    - country
    - total_revenue
    - completed_orders_count
    - unique_customers_count
    - avg_order_value

    Hints:
    - Use only completed orders.
    - Join orders with users by user_id.
    - Aggregate by order_date and country.
    - Use PySpark functions, not pandas.
    """

    # TODO 1: keep only completed orders
    # completed_orders_df = ...

    # TODO 2: join orders with users to get country
    # joined_df = ...

    # TODO 3: group by order_date and country
    # result_df = ...

    # TODO 4: select columns in the expected order
    # return result_df.select(...)

    raise NotImplementedError("Implement transform() function")