#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os 


if __name__ == "__main__":
    # Config
    user = "root"
    password = "root"
    host = "localhost",
    port = "5432"
    db = "postgresql"
    cmd1 = "wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
    filename_1 = "green_tripdata_2019-09.csv"
    table_name_1 = "green_tripdata"
    cmd2 = "wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
    filename_2 = "taxi+_zone_lookup.csv"
    table_name_2 = "taxi_zone"
    dbname = "ny_taxi"


# The configuration is added in order to have defined the data sources

# In[2]:


os.system("rm *.csv")
os.system(f"{cmd1} | {cmd2}")
os.system(f"gunzip {filename_1}.gz")


# In[3]:


df1 = pd.read_csv(filename_1, nrows=100)
df2 = pd.read_csv(filename_2, nrows=100)


# In[4]:


df1


# In[5]:


df2


# In[6]:


print(pd.io.sql.get_schema(df1, name=table_name_1))
print(pd.io.sql.get_schema(df2, name=table_name_2))


# In[7]:


df1.lpep_dropoff_datetime = pd.to_datetime(df1.lpep_dropoff_datetime)
df1.lpep_pickup_datetime = pd.to_datetime(df1.lpep_pickup_datetime)
print(pd.io.sql.get_schema(df1, name=table_name_1))


# In[8]:


from sqlalchemy import create_engine


# In[9]:


engine = create_engine(f'{db}://{user}:{password}@localhost:{port}/{dbname}')
print(f'{db}://{user}:{password}@localhost:{port}/{dbname}')


# docker run -it \
#     -e POSTGRES_USER="root" \
#     -e POSTGRES_PASSWORD="root" \
#     -e POSTGRES_DB="ny_taxi" \
#     -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
#     -p 5432:5432 \
#     postgres:13

# In[10]:


engine.connect()


# In[11]:


df_iter1 = pd.read_csv(filename_1, iterator=True, chunksize=100000)
df_iter2 = pd.read_csv(filename_2, iterator=True, chunksize=100000)
df_iter2


# In[12]:


df1 = next(df_iter1)
df2 = next(df_iter2)
df2


# In[13]:


# we need to provide the table name, the connection and what to do if the table already exists
# we choose to replace everything in case you had already created something by accident before.
df1.head(n=0).to_sql(name=table_name_1, con=engine, if_exists='replace')
df2.head(n=0).to_sql(name=table_name_2, con=engine, if_exists='replace')


# In[14]:


get_ipython().run_line_magic('time', "df1.to_sql(name=table_name_1, con=engine, if_exists='append')")


# In[15]:


get_ipython().run_line_magic('time', "df2.to_sql(name=table_name_2, con=engine, if_exists='append')")


# In[21]:


from time import time

def addToSql(df, df_iter, title, hasDates ):
  while True: 
    try:
        t_start = time()
        df = next(df_iter)

        if bool:
          df.lpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
          df.lpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=title, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))
    except StopIteration:
        print('completed')
        break
    
addToSql(df1, df_iter1, table_name_1,True)
addToSql(df2, df_iter2, table_name_2, False)

