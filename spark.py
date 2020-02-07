from pyspark.sql import SparkSession
from sklearn.externals import joblib
from sklearn import preprocessing
import numpy as np
import pandas as pd
import pyarrow
import pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, StringType, ArrayType, IntegerType, FloatType
import time
from pyspark.sql import *
from pyspark.sql.types import *
import ReadData

start_time = time.time()

le = preprocessing.LabelEncoder()

#Creating spark conatext
#spark = SparkSession.builder.getOrCreate()

sc = SparkSession.builder.master("spark://192.168.0.169:31618").getOrCreate()
#pyspark.SparkConf().setAppName('abcd').setMaster('spark://192.168.0.169:31618')
#sc = pyspark.SparkContext(conf=conf)
sqlContext = pyspark.SQLContext(sc)
    #Loading Models
try:
    loaded_model = joblib.load("/home/vaibhav/Desktop/aimodel.joblib")

    #print(pd_data)

    spark_df = ReadData.read_data()

    spark_df.show(10)
    """
    print(spark_df)
    df1 = sqlContext.read.csv("/home/vaibhav/Documents/data.csv",
                                    header='true',
                                    inferSchema='true')
    
    
    start_time = time.time()
    pandas_df = df1.toPandas()
    
    #print(pandas_df)
    
    result = loaded_model.predict(pandas_df)
    #print(result)
        
        
        with open("/home/vaibhav/Desktop/result.txt","a") as file:
            time = str(time.time() - start_time)
            file.write("for pandas" + time)"""
finally:
        sc.stop()