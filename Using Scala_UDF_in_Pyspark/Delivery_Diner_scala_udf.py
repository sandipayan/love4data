import os,sys
import time
import pytz
import datetime
import argparse
import logging

from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import _to_seq,  _to_java_column
from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import broadcast

#from marketing_mart.CRM.delivery_diner.ddls import *
#from marketing_mart.helpers import write_and_partition
#from marketing_mart.CRM.diner_last_address.ddls import *

logging.basicConfig(level=logging.INFO)
formatter = logging.Formatter("%(asctime)s %(levelname)s:%(name)s: %(message)s")
root_logger = logging.getLogger("[CRM ETL Delivery Diner]")

ACTIVE_LOOK_BACK = 380
# MEMORY_AND_DISK = StorageLevel(True, True, False, False)
MEMORY_ONLY = StorageLevel(False, True, False, False)


geom_table = 'source_mysql_core.geom'
customer_table = 'source_mysql_core.customer'
postal_code_dim_table = 'integrated_core.postal_code_dim'
diner_order_agg_table = 'integrated_diner.diner_order_agg'
diner_last_address_table = 'migrated_marketing_reporting.diner_last_address'
login_user_table = 'source_mysql_core.login_user'




#self.sc.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
#spark.conf.set("spark.kryoserializer.buffer.mb", "300")
#spark.conf.set("spark.kryoserializer.buffer.max", "300297910")



cbsa_rest_query = """
		SELECT DISTINCT c.cust_id AS restaurant_id 
		  , g.g AS wkt
		  , z.cbsa_name
		FROM {customer_table} c
		JOIN {geom_table} g 
		  ON c.cust_id = g.cust_id
		JOIN {zipcode_summary_table} z 
		  ON c.zip = z.postal_code
		WHERE c.test_restaurant <> TRUE
		  AND c.package_state_type_id IN (2, 3, 4, 5)
		  AND g LIKE 'POLYGON%'
		  AND z.cbsa_name='New York-Newark-Jersey City NY-NJ-PA'
	""".format(geom_table=geom_table, customer_table=customer_table,
               zipcode_summary_table=postal_code_dim_table)


spark.sql("""
select
d.diner_email as email,
max(last_order_date_ct) as last_order_date
from {diner_order_agg_table} d
GROUP BY diner_email
""".format(diner_order_agg_table=diner_order_agg_table)).repartition(200). \
    createOrReplaceTempView('diner_summary')

cbsa_diner_query = """
		SELECT dla.diner_id ,
			   cast(dla.lat as double) lat ,
			   cast(dla.lon as double) lon ,
			   z.cbsa_name
		FROM {diner_last_address} dla
		JOIN {login_user_table} lu 
		  ON lu.login_user_id = dla.diner_id
		JOIN {zipcode_summary_table} z 
		  ON z.postal_code = dla.zip
		JOIN diner_summary ds 
		  ON lu.email = ds.email
		WHERE ds.last_order_date >= (now() - interval '{day_lookback}' DAY)
		  AND dla.lat IS NOT NULL
		  AND dla.lon IS NOT NULL
		 AND z.cbsa_name='New York-Newark-Jersey City NY-NJ-PA'
	""".format(diner_last_address=diner_last_address_table, zipcode_summary_table=postal_code_dim_table,
               login_user_table=login_user_table,
               day_lookback=ACTIVE_LOOK_BACK)


diners_df = spark.sql(cbsa_diner_query).groupBy("cbsa_name").agg(F.collect_list(F.struct('diner_id','lat', 'lon')).alias('points'))

diners_df.persist(MEMORY_ONLY).count()

rest_df = spark.sql(cbsa_rest_query).repartition(1000)

rest_df.persist(MEMORY_ONLY).count()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 200485760)
joined_df = rest_df.join( broadcast(diners_df), rest_df['cbsa_name'] == diners_df['cbsa_name'], how='inner')



def test_udf(cols):
    _test_udf = sc._jvm.org.opensource.gis.polygon.PolygonUtils.scala_pip()
    return Column(_test_udf.apply(_to_seq(sc, cols, _to_java_column)))


delivery_diner_df = joined_df.withColumn('diners_list', test_udf(joined_df['wkt', 'points'])).select('restaurant_id', F.explode('diners_list').alias('diner_id')).select(F.col('diner_id').cast(LongType()), F.col('restaurant_id').cast(LongType()))

delivery_diner_df.count()


###  Chicago-Naperville-Elgin IL-IN-WI - 402 M, 19 Minutes --> --executor-memory 6G --driver-memory 8G --executor-cores 4 --num-executors 12
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 150485760)
# For this to be effective both the dataframe need to be cached else Spark does not have the stat and do NOT do broadcast
# https://stackoverflow.com/questions/53720134/why-spark-sql-is-not-doing-broadcast-join-even-when-size-under-autobroadcastjo
## pyspark --executor-memory 10G --driver-memory 8G --executor-cores 3 --num-executors 10 --deploy-mode client --jars scala-point-in-polygon.jar
## pyspark --executor-memory 10G --driver-memory 8G --executor-cores 1 --num-executors 10 --deploy-mode client --jars scala-point-in-polygon.jar --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryoserializer.buffer.max=512m

# map(lambda x: (x.diner_id, x.latitude, x.longitude),   numbers.select('s').rdd.map(lambda r: r[0]).collect()[0])

