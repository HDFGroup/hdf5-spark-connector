5parky:
This is a Spark DataSource for HDF5 files allowing you to read your HDF5 files
into Spark and do computations.

What’s Inside:

‘data’ - A couple HDF5 files that you can immediately test with.

‘doc’ - A PDF which includes numerous options you can use when reading your
        files as well as how your data/information will be constructed in a
        Spark DataFrame.

‘examples’ - Two Scala scripts that you can immediately run and see 5parky in
             action.

‘lib’ - The 5parky and JHDF5 JAR files.

Getting Started:

Make sure you have Spark installed:
	https://spark.apache.org/downloads.html

Running the Tests:

Run this command in the 5parky directory to run the example.scala script:
	spark-shell -i examples/example.scala --jars lib/5parky_2.11-0.0.1-ALPHA.jar,lib/sis-jhdf5-batteries_included.jar

And run this to run the virtual.scala script:
	spark-shell -i examples/virtual.scala --jars lib/5parky_2.11-0.0.1-ALPHA.jar,lib/sis-jhdf5-batteries_included.jar

The expected outputs are located at the bottom of this file, respectively.

Questions/Feedback:

Please help us continue to develop 5parky to best fit your needs by filling out
this questionnaire:
	SURVEYMONKEY
	https://confluence.hdfgroup.org/display/RandD/Questionnaire
Also feel free to reach out with questions at:
	help@hdfgroup.org

Version:
Beta version 1.0

Authors:
Gerd Heber
Alan Ren

License:
****************

Acknowledgements:
Data Source built upon Josh Asplund’s original spark-hdf5 project
(https://github.com/LLNL/spark-hdf5) with LLNL.


***(Output 1)***
Spark context Web UI available at http://10.192.138.70:4040
Spark context available as 'sc' (master = local[*], app id = local-1501859716404).
Spark session available as 'spark'.
Loading examples/example.scala...
import org.hdfgroup.spark.hdf5._
import org.apache.spark.sql.functions._
warning: there was one deprecation warning; re-run with -deprecation for details
sqlContext: org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLContext@152d2a58
files: String = data
dataset: String = /multi
test1: String = data/test1.h5
twodim: String = /dimensionality/2dim
With recursion:
df: org.apache.spark.sql.DataFrame = [FileID: int, Index: bigint ... 1 more field]
+------+-----+-----+
|FileID|Index|Value|
+------+-----+-----+
|     0|    0|   10|
|     0|    1|   11|
|     0|    2|   12|
|     0|    3|   13|
|     0|    4|   14|
|     0|    5|   15|
|     0|    6|   16|
|     0|    7|   17|
|     0|    8|   18|
|     0|    9|   19|
|     5|    0|   20|
|     5|    1|   21|
|     5|    2|   22|
|     5|    3|   23|
|     5|    4|   24|
|     5|    5|   25|
|     5|    6|   26|
|     5|    7|   27|
|     5|    8|   28|
|     5|    9|   29|
+------+-----+-----+
only showing top 20 rows

Without recursion:
df: org.apache.spark.sql.DataFrame = [FileID: int, Index: bigint ... 1 more field]
+------+-----+-----+
|FileID|Index|Value|
+------+-----+-----+
|     0|    0|    0|
|     0|    1|    1|
|     0|    2|    2|
|     0|    3|    3|
|     0|    4|    4|
|     0|    5|    5|
|     0|    6|    6|
|     0|    7|    7|
|     0|    8|    8|
|     0|    9|    9|
|     1|    0|   10|
|     1|    1|   11|
|     1|    2|   12|
|     1|    3|   13|
|     1|    4|   14|
|     1|    5|   15|
|     1|    6|   16|
|     1|    7|   17|
|     1|    8|   18|
|     1|    9|   19|
+------+-----+-----+
only showing top 20 rows

Full dataset:
df: org.apache.spark.sql.DataFrame = [FileID: int, Index: bigint ... 1 more field]
+------+-----+-----+
|FileID|Index|Value|
+------+-----+-----+
|     0|    0|    0|
|     0|    1|    1|
|     0|    2|    2|
|     0|    3|    3|
|     0|    4|    4|
|     0|    5|    5|
|     0|    6|    6|
|     0|    7|    7|
|     0|    8|    8|
|     0|    9|    9|
|     0|   10|   10|
|     0|   11|   11|
|     0|   12|   12|
|     0|   13|   13|
|     0|   14|   14|
|     0|   15|   15|
|     0|   16|   16|
|     0|   17|   17|
|     0|   18|   18|
|     0|   19|   19|
+------+-----+-----+
only showing top 20 rows

Hyperslab:
df: org.apache.spark.sql.DataFrame = [FileID: int, Index: bigint ... 1 more field]
+------+-----+-----+
|FileID|Index|Value|
+------+-----+-----+
|     0|   11|   11|
|     0|   12|   12|
|     0|   21|   21|
|     0|   22|   22|
+------+-----+-----+



***(Output 2)***
Spark context Web UI available at http://10.192.138.70:4040
Spark context available as 'sc' (master = local[*], app id = local-1501859747149).
Spark session available as 'spark'.
Loading examples/virtual.scala...
import org.hdfgroup.spark.hdf5._
import org.apache.spark.sql.functions._
warning: there was one deprecation warning; re-run with -deprecation for details
sqlContext: org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLContext@152d2a58
files: String = data/sub
sparky://files
df: org.apache.spark.sql.DataFrame = [FileID: int, FilePath: string ... 1 more field]
+------+----------------------------------------------------+--------+
|FileID|FilePath                                            |FileSize|
+------+----------------------------------------------------+--------+
|0     |/Volumes/Transcend/Download/5parky/data/sub/test2.h5|5536    |
|1     |/Volumes/Transcend/Download/5parky/data/sub/test3.h5|5536    |
|2     |/Volumes/Transcend/Download/5parky/data/sub/test1.h5|33962   |
+------+----------------------------------------------------+--------+

sparky://datasets
df: org.apache.spark.sql.DataFrame = [FileID: int, DatasetPath: string ... 3 more fields]
+------+--------------------+-----------+----------+------------+
|FileID|DatasetPath         |ElementType|Dimensions|ElementCount|
+------+--------------------+-----------+----------+------------+
|0     |/multi              |Int32      |[10]      |10          |
|1     |/multi              |Int32      |[10]      |10          |
|2     |/datatypes/float32  |Float32    |[10]      |10          |
|2     |/datatypes/float64  |Float64    |[10]      |10          |
|2     |/datatypes/int16    |Int16      |[10]      |10          |
|2     |/datatypes/int32    |Int32      |[10]      |10          |
|2     |/datatypes/int64    |Int64      |[10]      |10          |
|2     |/datatypes/int8     |Int8       |[10]      |10          |
|2     |/datatypes/string   |FLString   |[10]      |10          |
|2     |/datatypes/uint16   |UInt16     |[10]      |10          |
|2     |/datatypes/uint32   |UInt32     |[10]      |10          |
|2     |/datatypes/uint8    |UInt8      |[10]      |10          |
|2     |/dimensionality/1dim|Int32      |[10]      |10          |
|2     |/dimensionality/2dim|Int32      |[3, 10]   |30          |
|2     |/dimensionality/3dim|Int32      |[2, 2, 5] |20          |
|2     |/multi              |Int32      |[10]      |10          |
+------+--------------------+-----------+----------+------------+

sparky://attributes
df: org.apache.spark.sql.DataFrame = [FileID: int, ObjectPath: string ... 3 more fields]
+------+--------------------+-------------+-----------+----------+
|FileID|ObjectPath          |AttributeName|ElementType|Dimensions|
+------+--------------------+-------------+-----------+----------+
|0     |/multi              |Units        |FLString   |[]        |
|1     |/multi              |Units        |FLString   |[]        |
|2     |/dimensionality/1dim|Length       |FLString   |[]        |
|2     |/dimensionality/2dim|Area         |FLString   |[]        |
|2     |/dimensionality/3dim|Volume       |FLString   |[]        |
|2     |/multi              |Units        |FLString   |[]        |
+------+--------------------+-------------+-----------+----------+
