from pyspark.sql import SparkSession
import pyspark
from sklearn.externals import joblib
import time
from pyspark.sql import SQLContext
from pyspark import SparkContext
import ReadData

try:
    sc = SparkSession.builder.appName("abc").master("spark://192.168.0.169:31618").getOrCreate()
    #sc = pyspark.SparkContext(conf=conf)
    #sqlContext = pyspark.SQLContext(sc)
    sqlContext = SQLContext(sc)

    loaded_model = joblib.load("/home/vaibhav/Desktop/aimodel.joblib")
    start_time = time.time()
    df = ReadData.read_data()
    spark_df = sc.createDataFrame(df)
    spark_df.show(10)


finally:
    sc.stop()
