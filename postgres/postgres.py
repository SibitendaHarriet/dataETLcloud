from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import streamlit as st
import glob
import os

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

engine = create_engine("postgresql://<username>:<password>@localhost:5432/postdbt")
engine_dataset = create_engine("postgresql://<username>:<password>@localhost:5432/datasets_db")
engine.execute("CREATE TABLE IF NOT EXISTS records (date text NOT NULL, time text NOT NULL,
  station_id  double NOT NULL,
  col3 double DEFAULT NULL,
  col4 double DEFAULT NULL,
  col5 double DEFAULT NULL,
  col6 double DEFAULT NULL,
  col7 double DEFAULT NULL,
  col8 double DEFAULT NULL,
  col9 double DEFAULT NULL,
  col10 double DEFAULT NULL,
  col11 double DEFAULT NULL,
  col12 double DEFAULT NULL,
  col13 double DEFAULT NULL,
  col14 double DEFAULT NULL,
  col15 double DEFAULT NULL,
  col16 double DEFAULT NULL,
  col17 double DEFAULT NULL,
  col18 double DEFAULT NULL,
  col19 double DEFAULT NULL,
  col20 double DEFAULT NULL,
  col21 double DEFAULT NULL,
  col22 double DEFAULT NULL,
  col23 double DEFAULT NULL,
  col24 double DEFAULT NULL,
  col25 double DEFAULT NULL,
  col26 double DEFAULT NULL)")

def create_db(db_host, db_user, db_password, db_name):
    """create a new database
    1. Create connection to Postgresql
    """
    conn = psycopg2.connect(host = db_host,user = db_user,password = db_password, database = 'postdbt')
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("CREATE DATABASE %s  ;" % db_name)
    cur.close()


def write_record(name,details,engine):
    engine.execute("INSERT INTO records (name,details) VALUES ('%s','%s')" % (name,details))

def read_record(field,name,engine):
    result = engine.execute("SELECT %s FROM records WHERE name = '%s'" % (field,name))
    return result.first()[0]
    
def update_record(field,name,new_value,engine):
    engine.execute("UPDATE records SET %s = '%s' WHERE name = '%s'" % (field,new_value,name))

def write_dataset(name,dataset,engine):
    dataset.to_sql('%s' % (name),engine,index=False,if_exists='replace',chunksize=1000)

def read_dataset(name,engine):
    try:
        dataset = pd.read_sql_table(name,engine)
    except:
        dataset = pd.DataFrame([])
    return dataset

def list_datasets(engine):
    datasets = engine.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;")
    return datasets.fetchall()

st.title('Dashboard')
column_1, column_2 = st.beta_columns(2)

with column_1:
    st.header('Save records')
    name = st.text_input('Please enter name')
    details = st.text_input('Please enter details (separated by comma)')
    details = ('{%s}' % (details))
    if st.button('Save record to database'):
        write_record(name,details,engine)
        st.info('Name: **%s** and details: **%s** saved to database' % (name,details[1:-1]))

    st.header('Update records')
    field = st.selectbox('Please select field to update',('name','details'))
    name_key = st.text_input('Please enter name of record to be updated')    
    if field == 'name':
        updated_name = st.text_input('Please enter updated name')
        if st.button('Update records'):
            update_record(field,name_key,updated_name,engine)
            st.info('Updated name to **%s** in record **%s**' % (updated_name,name_key))                
    elif field == 'details':
        updated_details = st.text_input('Please enter updated details (separated by comma)')
        updated_details = ('{%s}' % (updated_details))  
        if st.button('Update records'):
            update_record(field,name_key,updated_details,engine)
            st.info('Updated details to  **%s** in record **%s**' % (updated_details[1:-1],name_key))
            
    st.header('Read records')
    record_to_read = st.text_input('Please enter name of record to read')
    if st.button('Search'):
        read_name = read_record('name',record_to_read,engine)
        read_details = read_record('details',record_to_read,engine)
        st.info('Record name is **%s**, record details is **%s**' % (read_name,str(read_details)[1:-1]))

with column_2:
    st.header('Save datasets')
    dataset = st.file_uploader('Please upload dataset')
    if dataset is not None:
        dataset = pd.read_csv(dataset)
        dataset_name = st.text_input('Please enter name for dataset')
        if st.button('Save dataset to database'):
            write_dataset('%s' % (dataset_name),dataset,engine_dataset)
            st.info('**%s** saved to database' % (dataset_name))

    try:
        read_title = st.empty()
        dataset_to_read = st.selectbox('Please select dataset to read',([x[0] for x in list_datasets(engine_dataset)]))
        read_title.header('Read datasets')
        if st.button('Read dataset'):
            df = read_dataset(dataset_to_read,engine_dataset)
            st.subheader('Chart')
            st.line_chart(df['value'])
            st.subheader('Dataframe')
            st.write(df)    
    except:
        pass