from sklearn.externals import joblib
import pandas as pd

x_test = [12,"sd","hi","ohh","sharma"]

pd.DataFrame(x_test)
df_test = pd.DataFrame([x_test], columns=["CUSTOMER_KEY", "CUSTOMER_TYPE", "FIRSTNAME", "GBI", "LASTNAME"])

loaded_model = joblib.load("/home/ubuntu/Documents/datanext/datanext/src/MLService/AIEngine/models/model_123_LogisticRegression/aimodel.joblib")

loaded_model.predict(df_test)

print(loaded_model)
