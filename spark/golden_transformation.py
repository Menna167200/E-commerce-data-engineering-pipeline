import os
from pyspark.sql import SparkSession

SILVER_PATH = 's3a://ecommerce-lake/silver/processed_data'
GOLD_BASE = 's3a://ecommerce-lake/gold'

spark = (
    SparkSession.builder
    .appName('Gold Transformation')
    .getOrCreate()
)

silver = spark.read.format('delta').load(SILVER_PATH)
silver.createOrReplaceTempView('cleaned_data') 


#fact_purchases 
fact_purchases = spark.sql(
    '''
    SELECT *, DATE(event_time) purchase_date
    FROM cleaned_data
    WHERE event_type = 'purchase'
    '''
    )
fact_purchases.createOrReplaceTempView('fact_purchases')
fact_purchases.write.mode('overwrite').format('delta').save(f'{GOLD_BASE}/fact_purchases')


# dim_users
dim_users = spark.sql(
    '''
    SELECT 
        user_id, 
        COUNT(DISTINCT user_session) total_orders, 
        ROUND(SUM(price), 2) total_spent,
        MIN(event_time) first_purchase_date, 
        MAX(event_time) last_purchase_date,
        ROUND(SUM(price) / COUNT(DISTINCT user_session), 2) AS avg_order_value
    FROM fact_purchases
    GROUP BY user_id
    '''
)
dim_users.write.mode('overwrite').format('delta').save(f'{GOLD_BASE}/dim_users')

# dim_products
dim_products = spark.sql(
    '''
    SELECT 
        product_id,
        category_id,
        category_code,
        brand,
        ROUND(AVG(price), 2) avg_price,
        COUNT(*) total_purchases
    FROM fact_purchases
    GROUP BY product_id,category_id,category_code,brand
    '''
    )
dim_products.write.mode('overwrite').format('delta').save(f'{GOLD_BASE}/dim_products')

# agg_daily_metrics 
agg_daily_metrics = spark.sql(
    '''
    SELECT 
        purchase_date,
        ROUND(SUM(price), 2) total_revenue,
        COUNT(DISTINCT user_session) total_orders,
        COUNT(*) total_items_sold,
        COUNT(DISTINCT user_id) unique_buyers,
        ROUND(AVG(price), 2) avg_order_value
    FROM fact_purchases
    GROUP BY purchase_date
    '''
    )
agg_daily_metrics.write.mode('overwrite').format('delta').save(f'{GOLD_BASE}/agg_daily_metrics')

print('Golden transformation completed')
spark.stop()
