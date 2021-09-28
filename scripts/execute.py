
from __future__ import print_function
from datetime import date, datetime, timedelta
import mysql.connector
from mysql.connector import errorcode
import pandas as pd



def DBConnect(dbName=None):
 
    conn = mysql.connector.connect(host='localhost', user='root',database=dbName, buffered=True)
    cur = conn.cursor()
    return conn, cur


def preprocess_df(df: pd.DataFrame) -> pd.DataFrame:

    
    
    try:
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.date
        
    except KeyError as e:
        print("Error:", e)

    return df

def insert_to_time_table(dbName: str, df: pd.DataFrame, table_name: str) -> None:
    print('=============inserting time data============================')

    conn, cur = DBConnect(dbName)

    df = preprocess_df(df)
    
    for _, row in df.iterrows():
        sqlQuery = f"""INSERT INTO {table_name} (date, dayofweek, hour, minute, seconds)
             VALUES( %s, %s, %s, %s, %s);"""
        data = (row[0], row[8], row[9], row[10], row[11])

        try:
            # Execute the SQL command
            cur.execute(sqlQuery, data)
            # Commit your changes in the database
            conn.commit()
            print("Data Inserted Successfully")
        except Exception as e:
            conn.rollback()
            print("Error: ", e)
    print('=============Time data insert complete===============================')
    return

def insert_to_flow_table(dbName: str, df: pd.DataFrame, table_name: str) -> None:
    print('=============inserting flow data============================')

    conn, cur = DBConnect(dbName)

    df = preprocess_df(df)
    
    for _, row in df.iterrows():
        sqlQuery = f"""INSERT INTO {table_name} (flow1, flow2, flow3, flowtotal)
             VALUES( %s, %s, %s, %s);"""
        data = (row[1], row[3], row[5], row[7])

        try:
            # Execute the SQL command
            cur.execute(sqlQuery, data)
            # Commit your changes in the database
            conn.commit()
            print("Data Inserted Successfully")
        except Exception as e:
            conn.rollback()
            print("Error: ", e)
    return
print('=============flow data insert complete===============================')

def insert_to_occupancy_table(dbName: str, df: pd.DataFrame, table_name: str) -> None:
    print('=============inserting occupancy data============================')

    conn, cur = DBConnect(dbName)

    df = preprocess_df(df)
    
    for _, row in df.iterrows():
        sqlQuery = f"""INSERT INTO {table_name} (occupancy1, occupancy2, occupancy3)
             VALUES( %s, %s, %s);"""
        data = (row[2], row[4], row[6])

        try:
            # Execute the SQL command
            cur.execute(sqlQuery, data)
            # Commit your changes in the database
            conn.commit()
            print("Data Inserted Successfully")
        except Exception as e:
            conn.rollback()
            print("Error: ", e)
    return
print('=============occupancy data insert complete===============================')

def insert_to_180sations_table(dbName: str, df: pd.DataFrame, table_name: str) -> None:
    print('=============inserting 180stations data============================')

    conn, cur = DBConnect(dbName)

    df2 = preprocess_df(df)

    for _, row in df2.iterrows():
        sqlQuery = f"""INSERT INTO {table_name} (ID,Fwy,Dir,District,County,City,State_PM,Abs_PM,Latitude,Longitude,Length,Type,Lanes,Name,User_ID_1,User_ID_2,User_ID_3,User_ID_4)
             VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        data = (row[1], row[2], row[3],row[4], row[5], row[6],row[7], row[8], row[9],row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18] )

        try:
            # Execute the SQL command
            cur.execute(sqlQuery, data)
            # Commit your changes in the database
            conn.commit()
            print("Data Inserted Successfully")
        except Exception as e:
            conn.rollback()
            print("Error: ", e)
    return
print('=============180stations data insert complete===============================')


if __name__ == "__main__":
    df = pd.read_csv(r'C:\Users\DELL\Documents\dataeng\data\richards.csv')
    df2 = pd.read_csv(r'C:\Users\DELL\Documents\dataeng\data\I80_stations.csv')
    DBConnect('sensordata')
    insert_to_time_table(dbName='sensordata', df=df, table_name='time')
    insert_to_flow_table(dbName='sensordata', df=df, table_name='flow')
    insert_to_occupancy_table(dbName='sensordata', df=df, table_name='ocuppancy')
    insert_to_occupancy_table(dbName='sensordata', df2=df2, table_name='180stations')