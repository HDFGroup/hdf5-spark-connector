5parky:
This is a Spark DataSource for HDF5 files. It lets one read data stored in HDF5
(or NetCDF-4) files located in a LOCAL file system (HDFS coming soon!) into Spark
and do, well, Spark things.

What's Inside:

data/ - A few HDF5 sample files

doc/ - A brief on the DataSource options

examples/ - A Scala script ready to be run with the Spark shell

lib/ - The 5parky and JHDF5 JAR files.

Getting Started:

Make sure you have Spark installed:
	https://spark.apache.org/downloads.html

Running the example:

./run-examples.sh

The expected output is shown at the bottom of this file.

Questions/Feedback:

help@hdfgroup.org

Version:
Beta version 2.0

Authors:
Gerd Heber
Alan Ren
Joe Lee

License:
****************

Acknowledgements:
Data Source built upon Josh Asplund's original spark-hdf5 project
(https://github.com/LLNL/spark-hdf5) with LLNL.


***(Output)***

./run-examples.sh 

data:
sub  test1.h5  test2.h5  test3.h5

data/sub:
test1.hdf5  test2.hdf5  test3.hdf5

2018-11-16 13:55:51 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark context Web UI available at http://jelly.ad.hdfgroup.org:4040
Spark context available as 'sc' (master = local[*], app id = local-1542398156311).
Spark session available as 'spark'.
Loading examples/basics.scala...
import org.hdfgroup.spark.hdf5._
import org.apache.spark.sql.functions._
warning: there was one deprecation warning; re-run with -deprecation for details
sqlContext: org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLContext@50fd739d
baseDir: String = data
subDir: String = data/sub
===>> data/**.[h5,hdf5] @ sparky://files

df: org.apache.spark.sql.DataFrame = [FileID: int, FilePath: string ... 1 more field]
+------+----------------------------------------------------------------------+--------+
|FileID|FilePath                                                              |FileSize|
+------+----------------------------------------------------------------------+--------+
|0     |/mnt/wrk/gheber/Bitbucket/5parky/pre-release/beta2/data/test2.h5      |1440    |
|5     |/mnt/wrk/gheber/Bitbucket/5parky/pre-release/beta2/data/sub/test2.hdf5|5536    |
|1     |/mnt/wrk/gheber/Bitbucket/5parky/pre-release/beta2/data/test3.h5      |1440    |
|2     |/mnt/wrk/gheber/Bitbucket/5parky/pre-release/beta2/data/test1.h5      |23722   |
|3     |/mnt/wrk/gheber/Bitbucket/5parky/pre-release/beta2/data/sub/test1.hdf5|33962   |
|4     |/mnt/wrk/gheber/Bitbucket/5parky/pre-release/beta2/data/sub/test3.hdf5|5536    |
+------+----------------------------------------------------------------------+--------+

===>> data/*.h5 @ sparky://datasets

df: org.apache.spark.sql.DataFrame = [FileID: int, DatasetPath: string ... 3 more fields]
+------+--------------------+-----------+----------+------------+
|FileID|DatasetPath         |ElementType|Dimensions|ElementCount|
+------+--------------------+-----------+----------+------------+
|0     |/datatypes/float32  |Float32    |[10]      |10          |
|0     |/datatypes/float64  |Float64    |[10]      |10          |
|0     |/datatypes/int16    |Int16      |[10]      |10          |
|0     |/datatypes/int32    |Int32      |[10]      |10          |
|0     |/datatypes/int64    |Int64      |[10]      |10          |
|0     |/datatypes/int8     |Int8       |[10]      |10          |
|0     |/datatypes/string   |FLString   |[10]      |10          |
|0     |/datatypes/uint16   |UInt16     |[10]      |10          |
|0     |/datatypes/uint32   |UInt32     |[10]      |10          |
|0     |/datatypes/uint8    |UInt8      |[10]      |10          |
|0     |/dimensionality/1dim|Int32      |[10]      |10          |
|0     |/dimensionality/2dim|Int32      |[3, 10]   |30          |
|0     |/dimensionality/3dim|Int32      |[2, 2, 5] |20          |
|0     |/multi              |Int32      |[10]      |10          |
|1     |/multi              |Int32      |[10]      |10          |
|2     |/multi              |Int32      |[10]      |10          |
+------+--------------------+-----------+----------+------------+

===>> data/sub/*.hdf5 @ sparky://attributes


df: org.apache.spark.sql.DataFrame = [FileID: int, ObjectPath: string ... 4 more fields]
+------+--------------------+-------------+-----------+----------+-----------+
|FileID|ObjectPath          |AttributeName|ElementType|Dimensions|Value      |
+------+--------------------+-------------+-----------+----------+-----------+
|0     |/dimensionality/1dim|Length       |FLString   |[]        |Meters     |
|0     |/dimensionality/2dim|Area         |FLString   |[]        |Meters     |
|0     |/dimensionality/3dim|Volume       |FLString   |[]        |Meters     |
|0     |/multi              |Units        |FLString   |[]        |Meters     |
|1     |/multi              |Units        |FLString   |[]        |Centimeters|
|2     |/multi              |Units        |FLString   |[]        |Millimeters|
+------+--------------------+-------------+-----------+----------+-----------+

===>> data/**.[h5,hdf5] @ /multi

df: org.apache.spark.sql.DataFrame = [FileID: int, Index: bigint ... 1 more field]
+------+-----+-----+
|FileID|Index|Value|
+------+-----+-----+
|0     |0    |10   |
|0     |1    |11   |
|0     |2    |12   |
|0     |3    |13   |
|0     |4    |14   |
|0     |5    |15   |
|0     |6    |16   |
|0     |7    |17   |
|0     |8    |18   |
|0     |9    |19   |
|5     |0    |10   |
|5     |1    |11   |
|5     |2    |12   |
|5     |3    |13   |
|5     |4    |14   |
|5     |5    |15   |
|5     |6    |16   |
|5     |7    |17   |
|5     |8    |18   |
|5     |9    |19   |
|1     |0    |20   |
|1     |1    |21   |
|1     |2    |22   |
|1     |3    |23   |
|1     |4    |24   |
|1     |5    |25   |
|1     |6    |26   |
|1     |7    |27   |
|1     |8    |28   |
|1     |9    |29   |
|2     |0    |0    |
|2     |1    |1    |
|2     |2    |2    |
|2     |3    |3    |
|2     |4    |4    |
|2     |5    |5    |
|2     |6    |6    |
|2     |7    |7    |
|2     |8    |8    |
|2     |9    |9    |
|3     |0    |0    |
|3     |1    |1    |
|3     |2    |2    |
|3     |3    |3    |
|3     |4    |4    |
|3     |5    |5    |
|3     |6    |6    |
|3     |7    |7    |
|3     |8    |8    |
|3     |9    |9    |
|4     |0    |20   |
|4     |1    |21   |
|4     |2    |22   |
|4     |3    |23   |
|4     |4    |24   |
|4     |5    |25   |
|4     |6    |26   |
|4     |7    |27   |
|4     |8    |28   |
|4     |9    |29   |
+------+-----+-----+

===>> data/test1.h5,data/test1.hdf5 @ /dimensionality/2dim

df: org.apache.spark.sql.DataFrame = [FileID: int, Index: bigint ... 1 more field]
+------+-----+-----+
|FileID|Index|Value|
+------+-----+-----+
|2     |0    |0    |
|2     |1    |1    |
|2     |2    |2    |
|2     |3    |3    |
|2     |4    |4    |
|2     |5    |5    |
|2     |6    |6    |
|2     |7    |7    |
|2     |8    |8    |
|2     |9    |9    |
|2     |10   |10   |
|2     |11   |11   |
|2     |12   |12   |
|2     |13   |13   |
|2     |14   |14   |
|2     |15   |15   |
|2     |16   |16   |
|2     |17   |17   |
|2     |18   |18   |
|2     |19   |19   |
|2     |20   |20   |
|2     |21   |21   |
|2     |21   |21   |
|2     |22   |22   |
|2     |23   |23   |
|2     |24   |24   |
|2     |25   |25   |
|2     |26   |26   |
|2     |27   |27   |
|2     |28   |28   |
|2     |29   |29   |
|3     |0    |0    |
|3     |1    |1    |
|3     |2    |2    |
|3     |3    |3    |
|3     |4    |4    |
|3     |5    |5    |
|3     |6    |6    |
|3     |7    |7    |
|3     |8    |8    |
|3     |9    |9    |
|3     |10   |10   |
|3     |11   |11   |
|3     |12   |12   |
|3     |13   |13   |
|3     |14   |14   |
|3     |15   |15   |
|3     |16   |16   |
|3     |17   |17   |
|3     |18   |18   |
|3     |19   |19   |
|3     |20   |20   |
|3     |21   |21   |
|3     |22   |22   |
|3     |23   |23   |
|3     |24   |24   |
|3     |25   |25   |
|3     |26   |26   |
|3     |27   |27   |
|3     |28   |28   |
|3     |29   |29   |
+------+-----+-----+

===>> data/test1.h5,data/test1.hdf5 @ /dimensionality/2dim[1:3,1:3]

df: org.apache.spark.sql.DataFrame = [FileID: int, Index: bigint ... 1 more field]
+------+-----+-----+
|FileID|Index|Value|
+------+-----+-----+
|2     |11   |11   |
|2     |12   |12   |
|2     |21   |21   |
|2     |22   |22   |
|3     |11   |11   |
|3     |12   |12   |
|3     |21   |21   |
|3     |22   |22   |
+------+-----+-----+
