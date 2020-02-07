import mysql.connector
from mysql.connector import Error
import pandas as pd

def read_data():
    try:
        mydb = mysql.connector.connect(
            host="192.168.0.169",
            port="31501",
            user="root",
            passwd="acid",
            database="TestData"
        )
        mycursor = mydb.cursor()

        mycursor.execute("SELECT * FROM data")

        source_df = mycursor.fetchall()

        dfObj = pd.DataFrame(source_df,columns=("CUSTOMER_KEY","CUSTOMER_TYPE","FIRSTNAME_ENG","GBI","LASTNAME_ENG"))
        mycursor.close()
        return dfObj

    except Error as e:
        print("Error reading data from MySQL table", e)
    finally:
        print("Done")

read_data()


""""#from pyspark.sql import SQLContext
#from pyspark import SparkContext

#sc = SparkContext(appName="TestPySpark")
#sqlContext = SQLContext(sc)

source_df = sqlContext.read.format('jdbc').options(
          url='jdbc:mysql://192.168.0.169:31501/TestData',
          driver='com.mysql.jdbc.Driver',
          dbtable='data',
          user='root',
          password='acid').load()"""

