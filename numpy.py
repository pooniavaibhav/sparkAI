from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
import joblib
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, StringType, ArrayType, IntegerType, FloatType
import time
#import numpy as np
from sklearn.externals import joblib
from sklearn import preprocessing
from pyspark.sql.types import StructField,IntegerType, StructType,StringType
import pandas as pd

start_time = time.time()
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
#print(a)
with open("/home/vaibhav/Desktop/result.txt", "a") as file:
    time = str(time.time() - start_time)
    file.write("for udf" + time)

result = loaded_model.predict(a)

with open("/home/vaibhav/Desktop/result.txt", "a") as file:
    time = str(time.time() - start_time)
    file.write("for numpy" + time)

print(result)

"""@F.udf(returnType = FloatType())
def predict_udf(*cols):
    return float(loaded_model.predict(cols))
df_pred_a = source_df.select(
    F.col('id'),
    predict_udf(*columns).alias('salary'))

with open("/home/vaibhav/Desktop/result.txt", "a") as file:
    time = str(time.time() - start_time)
    file.write("for udf" + time)

#df_pred_a.show(10)"""