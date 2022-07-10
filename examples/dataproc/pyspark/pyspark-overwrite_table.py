#!/usr/bin/env python

from pyspark.sql import SparkSession

print("create spark session")

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('test-pyspark-on-dataproc') \
  .getOrCreate()

print("create temp bucket")
bucket = "orders-customers-bucket"
spark.conf.set('temporaryGcsBucket', bucket)

print("read data from BQ")
orderspayments = spark.read.format('bigquery').option('table', 'hardy-position-352014.ecommerce.orders-payments').load()
orderspayments.createOrReplaceTempView('ordersPayments')

print("aggregate data")
payment_sum = spark.sql('SELECT payment_type, SUM(payment_value) AS total_amount FROM ordersPayments GROUP BY payment_type')
payment_sum.show()
payment_sum.printSchema()

# Saving the data to BigQuery
print("save data to BQ")
payment_sum.write.format('bigquery').mode("overwrite").option('table', 'hardy-position-352014.ecommerce.payments_summary').save()