import os
import pandas as pd
import boto3
import redshift_connector
import awswrangler as wr
from boto3 import Session

def lambda_handler(event, context):
  session = Session()
  credentials = session.get_credentials()

  current_credentials = credentials.get_frozen_credentials()
  
  ACCESS_KEY = current_credentials.access_key
  SECRET_KEY = current_credentials.secret_key
  SESSION_TOKEN = current_credentials.token
  
  s3_client = boto3.client(
  's3',
  aws_access_key_id=ACCESS_KEY,
  aws_secret_access_key=SECRET_KEY,
  aws_session_token=SESSION_TOKEN)
  s3_bucket_name = 'deman4-group2-dataframe'
  
  s3 = boto3.resource(
  's3',
  aws_access_key_id=ACCESS_KEY,
  aws_secret_access_key=SECRET_KEY,
  aws_session_token=SESSION_TOKEN)
  
  my_bucket = s3.Bucket(s3_bucket_name)
  
  #Establishing Database Connection
  connection = redshift_connector.connect(
      host = "redshiftcluster-gyryx7hwpsmz.cv7hcrmjdnhd.eu-west-1.redshift.amazonaws.com",
      user = "group02",
      password = "Redshift-deman4-group02",
      port = 5439,
      database = "group02_cafe"
  )
  
  cursor = connection.cursor()
  cursor.execute("CREATE TABLE IF NOT EXISTS All_Data (id INT IDENTITY(1, 1) PRIMARY KEY,timestamp varchar(255) NOT NULL,store_name varchar(255) NOT NULL,total_price float NOT NULL,payment_method varchar(255) NOT NULL);")
  cursor.execute("CREATE TABLE IF NOT EXISTS Product_Info (product_id INT IDENTITY(1, 1) PRIMARY KEY,name varchar(255) NOT NULL,flavour varchar(255) NOT NULL,price float NOT NULL);")
  cursor.execute("CREATE TABLE IF NOT EXISTS Order_Info (id INT IDENTITY(1, 1) PRIMARY KEY,order_id int NOT NULL,product_id int NOT NULL,quantity int NOT NULL,price float NOT NULL, FOREIGN KEY (order_id) REFERENCES All_Data (id), FOREIGN KEY (product_id) REFERENCES Product_Info (product_id));")

  orders_table = wr.s3.read_csv('s3://deman4-group2-dataframes/orders_table.csv')
  main_table = wr.s3.read_csv('s3://deman4-group2-dataframes/main_table.csv')
  products_table = wr.s3.read_csv('s3://deman4-group2-dataframes/product_table.csv')
  
  #Exporting Data Frame into SQL Database
  for x in orders_table.values.tolist():
    cursor.execute(f"INSERT INTO All_Data (timestamp, store_name, total_price, payment_method) VALUES {tuple(x)}")
    
  #Exporting Product Information into SQL Database
  for x in products_table.values.tolist():
    cursor.execute(f"INSERT INTO Product_Info (name, flavour, price) VALUES {tuple(x)}")
      
  #Exporting Order Information into SQL Database
  for x in main_table.values.tolist():
    cursor.execute(f"INSERT INTO Order_Info (order_id, product_id, quantity, price) VALUES {tuple(x)}")
 
  connection.commit()    
  cursor.close()
  connection.close()