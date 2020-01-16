from sklearn.externals import joblib
from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

test_data = spark.createDataFrame([123,"abc","chipunk","abc","sharma"])

x_test=[123,"abc","chipunk","abc","sharma"]

loaded_model = joblib.load("/home/ubuntu/PycharmProjects/test/model_123_LogisticRegression/aimodel.joblib")

prediction = loaded_model.transform(test_data)

result = prediction.collect()

print(result)