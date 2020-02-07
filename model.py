import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.externals import joblib

dataset = pd.read_csv("/home/vaibhav/Documents/MachineLearning/Part 2 - Regression/Section 4 - Simple Linear Regression/Salary_Data.csv", index= False)
x = dataset.iloc[:,:-1]
y = dataset.iloc[:,1]

from sklearn.model_selection import train_test_split
x_train,x_test,y_train,y_test = train_test_split(x,y,test_size = 1/3, random_state = 0)

from sklearn.linear_model import LinearRegression
regressor = LinearRegression()
regressor.fit(x_train, y_train)

y_pred = regressor.predict(x_test) #the vector of predictions of dependent variables.

print(x_test)
print(y_pred)