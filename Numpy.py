from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
import joblib
import time
import numpy as np
from sklearn.externals import joblib

sc = SparkSession.builder.master("spark://192.168.0.169:31618").getOrCreate()

sqlContext = SQLContext(sc)

loaded_model = joblib.load("/home/vaibhav/PycharmProjects/spark/Linearmodel.pkl")

source_df = sqlContext.read.format('jdbc').options(
          url='jdbc:mysql://192.168.0.169:31501/TestData',
          driver='com.mysql.jdbc.Driver',
          dbtable='emp',
          user='root',
          password='acid').load()

years = source_df.select("YearsExperience")

a = np.array(years.select('YearsExperience').collect())

result = loaded_model.predict(a)

with open("/home/vaibhav/Desktop/result.txt", "a") as file:
    time = str(time.time() - start_time)
    file.write("for numpy" + time)
print(result)