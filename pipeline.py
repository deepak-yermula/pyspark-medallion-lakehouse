"""Local Bronze/Silver/Gold PySpark transformation using only Parquet."""
import argparse
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F, types as T

ORDER_SCHEMA = T.StructType([
    T.StructField('order_id', T.StringType()),
    T.StructField('customer_id', T.StringType()),
    T.StructField('order_date', T.StringType()),
    T.StructField('amount', T.StringType()),
    T.StructField('status', T.StringType()),
    T.StructField('updated_at', T.StringType()),
])
CUSTOMER_SCHEMA = T.StructType([
    T.StructField('customer_id', T.StringType()),
    T.StructField('country', T.StringType()),
])


def build(spark, data_dir, output_dir):
    """Build three medallion layers from the bundled synthetic data."""
    output_dir = str(output_dir)
    orders = spark.read.option('header', True).schema(ORDER_SCHEMA).csv(str(Path(data_dir) / 'orders.csv'))
    customers = spark.read.option('header', True).schema(CUSTOMER_SCHEMA).csv(str(Path(data_dir) / 'customers.csv'))
    orders.write.mode('overwrite').parquet(f'{output_dir}/bronze/orders')
    customers.write.mode('overwrite').parquet(f'{output_dir}/bronze/customers')

    raw = spark.read.parquet(f'{output_dir}/bronze/orders')
    typed = (raw.withColumn('amount_decimal', F.col('amount').cast(T.DecimalType(18, 2)))
               .withColumn('order_date_parsed', F.to_date('order_date'))
               .withColumn('updated_at_parsed', F.to_timestamp('updated_at')))
    valid = typed.filter(
        F.col('order_id').isNotNull()
        & (F.length(F.trim('order_id')) > 0)
        & F.col('customer_id').isNotNull()
        & (F.length(F.trim('customer_id')) > 0)
        & F.col('order_date_parsed').isNotNull()
        & F.col('updated_at_parsed').isNotNull()
        & F.col('amount_decimal').isNotNull()
        & (F.col('amount_decimal') >= F.lit(0))
        & F.col('status').isin('completed', 'cancelled', 'pending')
    )
    window = Window.partitionBy('order_id').orderBy(F.col('updated_at_parsed').desc())
    silver_orders = (valid.withColumn('_rank', F.row_number().over(window))
                          .filter(F.col('_rank') == 1)
                          .select('order_id', 'customer_id',
                                  F.col('order_date_parsed').alias('order_date'),
                                  F.col('amount_decimal').alias('amount'),
                                  'status', F.col('updated_at_parsed').alias('updated_at')))
    silver_customers = (spark.read.parquet(f'{output_dir}/bronze/customers')
        .filter(F.col('customer_id').isNotNull() & F.col('country').isNotNull())
        .dropDuplicates(['customer_id', 'country']))
    # This demo assumes exactly one country row per customer in source data.
    silver_orders.write.mode('overwrite').parquet(f'{output_dir}/silver/orders')
    silver_customers.write.mode('overwrite').parquet(f'{output_dir}/silver/customers')

    cleaned_orders = spark.read.parquet(f'{output_dir}/silver/orders')
    cleaned_customers = spark.read.parquet(f'{output_dir}/silver/customers')
    gold = (cleaned_orders.filter(F.col('status') == 'completed')
            .join(cleaned_customers, 'customer_id', 'inner')
            .withColumn('sales_month', F.date_format('order_date', 'yyyy-MM'))
            .groupBy('sales_month', 'country')
            .agg(F.sum('amount').alias('revenue'), F.count('*').alias('orders'))
            .orderBy('sales_month', 'country'))
    gold.write.mode('overwrite').parquet(f'{output_dir}/gold/monthly_revenue')
    return spark.read.parquet(f'{output_dir}/gold/monthly_revenue')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', default=str(Path(__file__).resolve().parent / 'data'))
    parser.add_argument('--output', default='output')
    args = parser.parse_args()
    spark = (SparkSession.builder.master('local[2]')
             .appName('DeepakRetailMedallion')
             .config('spark.sql.shuffle.partitions', '2')
             .getOrCreate())
    try:
        build(spark, args.data, args.output).show(truncate=False)
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
