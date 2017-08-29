## submit with spark2-submit --jars /lib/hadoop_lib/spark/elasticsearch-hadoop-5.5.1.jar spark_ingest.py
from pyspark.sql import HiveContext
from pyspark.sql.functions import to_date
from pyspark.sql.types import DataType
from pyspark import SparkContext, SparkConf, SQLContext

def main(sc):
  hive_context = HiveContext(sc)
  orders_joined_date = hive_context.table('electronics_data.orders_joined_date')
  orders_joined_date.registerTempTable('orders_joined_date')
  
  view = hive_context.sql('SELECT * from orders_joined_date') 
  ### TODO add where clause parameters for incrementals
  
  view.write.format("org.elasticsearch.spark.sql").option("es.resource", "electronics5/table").option("es.nodes", "rose").save(mode="overwrite")

  ## Or to append incremental loads
  ###view.write.format("org.elasticsearch.spark.sql").option("es.resource", "electronics5/table").option("es.nodes", "rose").save(mode="append")

if __name__ == "__main__":
  conf = SparkConf().setAppName("ingest_test")
  conf = conf.setMaster("local[*]")
  sc = SparkContext(conf=conf)
  main(sc)




sc.stop
