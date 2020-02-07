from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
import time
from sklearn.externals import joblib
import pandas as pd

sc = SparkSession.builder.master("spark://192.168.0.169:31618").getOrCreate()
sqlContext = SQLContext(sc)


loaded_model = joblib.load("/home/vaibhav/PycharmProjects/spark/Linearmodel.pkl")
start_time = time.time()
source_df = sqlContext.read.format('jdbc').options(
          url='jdbc:mysql://192.168.0.169:31501/TestData',
          driver='com.mysql.jdbc.Driver',
          dbtable='empout',
          user='root',
          password='acid').load()


years = source_df.select("YearsExperience")

pandas_df = years.toPandas()

# print(pandas_df)

result = loaded_model.predict(pandas_df)

#final = pd.DataFrame({'Salary': result[:, 0])

#with open("/home/vaibhav/Desktop/result.txt", "a") as file:

print(str(time.time() - start_time))

    #file.write("for pandas" + time)

"""result.write.format('jdbc').options(
      url='jdbc:mysql://192.168.0.169:31501/TestData',
      driver='com.mysql.jdbc.Driver',
      dbtable='empout',
      user='root',
      password='acid').mode('append').save()"""
