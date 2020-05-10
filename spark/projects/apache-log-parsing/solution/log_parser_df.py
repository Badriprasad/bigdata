from pyspark.sql.functions import *

sqldf = spark.read.text("/data/spark/project/NASA_access_log_Aug95.gz")
columns = split(sqldf["value"], " ")
df = sqldf.withColumn("host", columns.getItem(0)).withColumn("timestamp", columns.getItem(3)).withColumn("url", columns.getItem(6)).withColumn("httpcode", columns.getItem(8))
df = df.withColumn('timestamp',translate(col('timestamp'), '/[\[\]]', ''))
df = df.withColumn('timestamp',df.timestamp.substr(0,12))
df.createOrReplaceTempView("log")
spark.sql("select url, count(*) from log group by url order by count(*) desc").show(10)
spark.sql("select timestamp, count(*) from log group by timestamp order by count(*) desc").show(10)
spark.sql("select timestamp, count(*) from log group by timestamp order by count(*) asc").show(10)
spark.sql("select count(*) from log where httpcode ='403'").show()
