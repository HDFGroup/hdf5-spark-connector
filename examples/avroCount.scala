// time spark-shell -i examples/avroCount.scala --packages com.databricks:spark-avro_2.11:3.2.0

import com.databricks.spark.avro._

sc.setLogLevel("ERROR")

val df = spark.read.avro("df.avro")
df.count()

System.exit(0)
