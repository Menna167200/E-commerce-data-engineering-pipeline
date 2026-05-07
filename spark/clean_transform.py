import os
import sys
import json
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType
from pyspark.ml.feature import Imputer
from delta.tables import DeltaTable

SILVER_PATH = 's3a://ecommerce-lake/silver/processed_data'

if len(sys.argv) < 2:
    raise ValueError('Missing input argument')

ref_path = sys.argv[1]
with open(ref_path, 'r') as f:
    ref = json.load(f)

file_path = ref.get('file_path')
if not file_path:
    raise ValueError('file_path not found in ref JSON')


spark = (
    SparkSession.builder
    .appName('Cleaning Data')
    .getOrCreate()
)



print(f'Cleaning file: {file_path}')
df = spark.read.parquet(file_path)


def clean(df):
    VALID_EVENTS = ['view', 'cart', 'remove_from_cart', 'purchase']
    INVALID_VALUES = ['n/a', 'null', 'none', '']


    df = (
        df
        .dropDuplicates(['user_id', 'event_time', 'product_id'])
        .dropna(how='all')
        .dropna(subset=['user_id', 'event_time', 'product_id'])
    )

    df = (
        df
        .withColumn('event_time', F.to_timestamp('event_time'))
        .withColumn('price', F.col('price').cast('double'))
        .withColumn('user_id', F.col('user_id').cast('long'))
        .withColumn('product_id', F.col('product_id').cast('long'))
    )

    df = (
        df
        .withColumn('price', F.when(F.col('price') < 0, None).otherwise(F.col('price')))
    )

    imputer = Imputer(
        inputCols=['price'],
        outputCols=['price_imputed']
    ).setStrategy('median')

    df = (
        imputer.fit(df).transform(df)
        .drop('price')
        .withColumnRenamed('price_imputed', 'price')
    )


    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            cleaned_col = F.lower(F.trim(F.col(field.name)))
            
            df = df.withColumn(
                field.name,
                F.when(
                    (cleaned_col.isNull()) | (cleaned_col.isin(INVALID_VALUES)), f'{field.name}_missing'
                ).otherwise(cleaned_col)
            )

    df = df.filter(F.col('event_type').isin(VALID_EVENTS + ['event_type_missing']))

    return df

df = clean(df)

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    silver = DeltaTable.forPath(spark, SILVER_PATH)
    (
        silver.alias('existing')
        .merge(
            df.alias('incoming'),
            '''
            existing.user_id    = incoming.user_id    AND
            existing.event_time = incoming.event_time AND
            existing.product_id = incoming.product_id
            '''
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f'Merged into existing Delta table at {SILVER_PATH}')
else:
    df.write.format('delta').mode('overwrite').save(SILVER_PATH)
    print(f'Created new Delta table at {SILVER_PATH}')

spark.stop()