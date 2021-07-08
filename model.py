import psycopg2
from config import config
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import numpy as np

# Connect to the postgresql database with config.py and datbaase.ini file
connection = None
params = config()
connection = psycopg2.connect(**params)
crsr = connection.cursor()

#Reading the table from postgrs and create the pandas table
def data_table(sqlquery, database = connection):
    table = pd.read_sql_query(sqlquery, database)
    return table

#Function to create the table with select query statement
def pandas_table():
   data =  data_table("select limit_bal, sex, education, marriage, age, pay_0 as payment, bill_amt1 as BillAmount,pay_amt1 as payAmount,defualt_val as outcome from ucl_creditcard")
   return data

# Model fit funcation with features and target with random forest classifier
def model_fit():
   df =  pandas_table()
   x = df.iloc[:,:8]
   y = df.iloc[:,-1]
   clsfr = RandomForestClassifier()
   clsoutput  = clsfr.fit(x,y)
   return clsoutput
   
#predict model by getting the values for all the features
def predict_model():
    arr = np.array([])
    key_item = ['limit balance','sex(0 or 1)','education','marriage','age','payment','bill Amount','Payamout']
    for item in key_item:
        in_val = int(input('Enter the value for {0} : '.format(item)))
        arr = np.append(arr,in_val)
    x_t = np.reshape(arr,(1,-1))
    clsfr = model_fit()
    y_t = clsfr.predict(x_t)
    x_t = np.append(x_t,y_t)
    return x_t
    
# Collect the input and output value and insert in to the table    
def final_result(database = connection):
    try:
        insert_data =  predict_model()
        print(insert_data)
        postgres_insert_query = """ INSERT INTO predict_result (limit_balance, sex, education, 
        marriage,age, payment, billamount, payamount, result) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        record_to_insert = (insert_data)
        crsr.execute(postgres_insert_query, record_to_insert)
   
        connection.commit()

        count = crsr.rowcount
        print(count, "Record inserted successfully into result table")

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into mobile table", error)

    finally:
    # closing database connection.
        if connection:
            crsr.close()
            connection.close()
            print("PostgreSQL connection is closed")

if __name__ == "__main__":
    final_result()
