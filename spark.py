
from pyspark.sql import sparkSession
from pyspark.sql.types import StruckType,StructField, IntegerType, StringType
import pyspark.sql.functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

myschema = StructType([\
                       StructField("userID",IntegerType(), True),
                       StructField("name",StringType(), True),
                       StructField("age",IntegerType(), True),
                       StructField("Friends",IntegerType(), True),
                       ])
people = spark.read.format("csv")\
    .schema(myschema)\
    .option("path","hdfs:///user/maria_dev/spark/rawdata--.csv")\
    .load()

people.printSchema()

output = people.select(people.userID,people.name\
                       ,people.age,people.friends)\
         .where(people.age < 30).withColumn('insert_ts',func.current_timestamp())\
         .orderBy(people.userID).cache()

output.createOrReplaceTempView("people")

spark.sql("select * from people").show()

spark.sql("select userID, name from people where friends > 100 order by userID").show()

output.write\
.format("json").mode("overwrite")\
.option("path","hdfs:///user/maria_dev/spark/job_output/")\
.partitionBy("age")\
.save()
