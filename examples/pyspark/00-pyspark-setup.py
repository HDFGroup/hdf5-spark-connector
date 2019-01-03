# To work with IPython/PySpark from Org-mode Babel copy this file to
# ~/.ipython/profile_default/startup
import os
import sys

# Adjust the JAR paths!
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/gheber/work/Bitbucket/5parky/target/scala-2.11/5parky_2.11-0.0.2-BETA.jar,/home/gheber/work/Bitbucket/5parky/lib/sis-jhdf5-18.09.0-pre1.jar,/home/gheber/work/Bitbucket/5parky/lib/sis-base-18.08.0.jar pyspark-shell'
 
spark_home = os.environ.get('SPARK_HOME', None)
if not spark_home:
    raise ValueError('SPARK_HOME environment variable is not set')
sys.path.insert(0, os.path.join(spark_home, 'python'))
# Adjust the Py4J version!
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.7-src.zip'))
exec(open(os.path.join(spark_home, 'python/pyspark/shell.py')).read())
