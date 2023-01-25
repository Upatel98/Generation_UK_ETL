import os
import pandas as pd
import boto3
import awswrangler as wr
from boto3 import Session
from io import StringIO 
import datetime

def handler(event, context):
  from datetime import date, timedelta
  yesterday = date.today() - timedelta(days=1)
  d1 = yesterday.strftime('%Y/%-m/%-d')
  session = Session()
  credentials = session.get_credentials()
  current_credentials = credentials.get_frozen_credentials()
  
  ACCESS_KEY = current_credentials.access_key
  SECRET_KEY = current_credentials.secret_key
  SESSION_TOKEN = current_credentials.token
  
  drinks = {
    "Latte": [2.15, 2.45],
    "Flavoured latte - Vanilla": [2.55, 2.85],
    "Flavoured latte - Caramel": [2.55, 2.85],
    "Flavoured latte - Hazelnut": [2.55, 2.85],
    "Flavoured latte - Gingerbread": [2.55, 2.85],
    "Cappuccino": [2.15, 2.45],
    "Americano": [1.95, 2.25],
    "Flat white": [2.15, 2.45],
    "Cortado": [2.05, 2.35],
    "Mocha": [2.30, 2.70],
    "Espresso": [1.50, 1.80],
    "Filter coffee": [1.50, 1.80],
    "Chai latte": [2.30, 2.60],
    "Hot chocolate": [2.20, 2.90],
    "Flavoured hot chocolate - Caramel": [2.60, 2.90],
    "Flavoured hot chocolate - Hazelnut": [2.60, 2.90],
    "Flavoured hot chocolate - Vanilla": [2.60, 2.90],
    "Luxury hot chocolate": [2.40, 2.70],
    "Red Label tea": [1.20, 1.80],
    "Speciality Tea - Earl Grey": [1.30, 1.60],
    "Speciality Tea - Green": [1.30, 1.60],
    "Speciality Tea - Camomile": [1.30, 1.60],
    "Speciality Tea - Peppermint": [1.30, 1.60],
    "Speciality Tea - Fruit": [1.30, 1.60],
    "Speciality Tea - Darjeeling": [1.30, 1.60],
    "Speciality Tea - English breakfast": [1.30, 1.60],
    "Iced latte": [2.35, 2.85],
    "Flavoured iced latte - Vanilla": [2.75, 3.25],
    "Flavoured iced latte - Caramel": [2.75, 3.25],
    "Flavoured iced latte - Hazelnut": [2.75, 3.25],
    "Iced americano": [2.15, 2.50],
    "Frappes - Chocolate Cookie": [2.75, 3.25],
    "Frappes - Strawberries & Cream": [2.75, 3.25],
    "Frappes - Coffee": [2.75, 3.25],
    "Smoothies - Carrot Kick": [2.00, 2.50],
    "Smoothies - Berry Beautiful": [2.00, 2.50],
    "Smoothies - Glowing Greens": [2.00, 2.50],
    "Hot Chocolate": [1.40, 1.70],
    "Glass of milk": [0.70, 1.10]
    }
  
  new_drinks = {}
  for key, value in drinks.items():
    regular = value[0]
    large = value[1]
    new_drinks[f"Regular {key}"] = regular
    new_drinks[f"Large {key}"] = large
  
  drinks_list = []
  for keys, values in new_drinks.items():
    item = f"{keys} - {values:.2f}"
    drinks_list.append(item)

  s3_client = boto3.client(
  's3',
  aws_access_key_id=ACCESS_KEY,
  aws_secret_access_key=SECRET_KEY,
  aws_session_token=SESSION_TOKEN)
  s3_bucket_name = 'deman4-group2'
  
  s3 = boto3.resource(
  's3',
  aws_access_key_id=ACCESS_KEY,
  aws_secret_access_key=SECRET_KEY,
  aws_session_token=SESSION_TOKEN)
  
  my_bucket = s3.Bucket(s3_bucket_name)
  
  def iterate_files():
    df_list = []
    for file in my_bucket.objects.all():
      filename = file.key
      if filename.endswith(".csv") and filename.startswith(d1):
        df_list.append(wr.s3.read_csv(f's3://{s3_bucket_name}/{filename}', names=["Timestamp", "Store name", "Customername", "Basket id", "total price","Payment method", "Cardnumber"]))
        print(filename)
    return df_list
  
  final_df =pd.concat(iterate_files(), ignore_index=True)
  
  def drop_sensitive(df):
    return df.drop(["Customername", "Cardnumber"], axis=1)
  
  test_df = drop_sensitive(final_df)
  test_df = test_df.drop_duplicates()
  
  def index_list_maker(df):
    test_df_products = df["Basket id"].tolist()
    new_list = []
    for i in test_df_products:
        new_list.append(i.split(","))
    final_list = drinks_list
    index_list = []
    for i in new_list:
        ind_list = []
        for j in i:
            ind = 0
            for k in final_list:
                if j.strip() == k:
                    ind_list.append(ind)
                ind += 1
        index_list.append(ind_list)
    return index_list
  
  products_table = pd.DataFrame(drinks_list, columns=['product_name'])
  products_table = pd.DataFrame(drinks_list, columns=['product_name'])
  
  def replace_keys(df, index):
    index_list = index(df)
    for i in range(len(df["Basket id"])):
        df["Basket id"].iloc[i] = ' '.join(str(x) for x in index_list[i])
    return df
  
  df = replace_keys(test_df,index_list_maker)
  
  def product_table_normalizer(product_table):
    product_table[['product_name', "flavour", 'price']] = product_table['product_name'].str.split(pat='-',n=2,expand=True)
    for i in range(len(products_table["price"])):
        if product_table["price"].iloc[i] is None:
            product_table["price"].iloc[i] = products_table["flavour"].iloc[i]
            product_table["flavour"].iloc[i] = "None"
    return product_table
  
  products_table = product_table_normalizer(products_table)

  def orders_table_maker(df):
    order_list =[]
    for item in range(len(df["Basket id"])):
        order_item = 0
        order_dict = {}
        list = df["Basket id"].iloc[item].split()
        for items in list:
            order_item += 1
            order_dict = {}
            order_dict["Order Id"] = item + 1
            order_dict[f"Product Id"] = int(items) + 1
            order_list.append(order_dict)
    order_df = pd.DataFrame(order_list)
    return order_df
  
  def orders_table_count(df):
    cols = ["Order Id", 'Product Id']
    df['quantity'] = df.groupby(cols)["Order Id"].transform('size')
    return df
  
  main_table = orders_table_maker(df)
  main_table = orders_table_count(main_table)
  main_table = main_table.drop_duplicates()
  orders_table = df.drop(["Basket id"], axis=1)
  
  def price_add(df,pdf):
    print(len(df["Product Id"]))
    df["price"] = 0
    for i in range(len(df["Product Id"])):
        item = df["Product Id"].iloc[i] - 1
        id = pdf["price"].iloc[item]
        amount = df["quantity"].iloc[i]
        df["price"].iloc[i] = float(id) * float(amount)
    return df
  
  main_table = price_add(main_table, products_table)
  
  bucket = 'deman4-group2-dataframes' 
  csv_buffer = StringIO()
  csv_buffer2 = StringIO()
  csv_buffer3 = StringIO()
  main_table.to_csv(csv_buffer, index = False)
  products_table.to_csv(csv_buffer2,index = False)
  orders_table.to_csv(csv_buffer3,index = False)
  s3_resource = boto3.resource('s3')
  s3_resource.Object(bucket, 'main_table.csv').put(Body=csv_buffer.getvalue())
  s3_resource.Object(bucket, 'products_table.csv').put(Body=csv_buffer2.getvalue())
  s3_resource.Object(bucket, 'orders_table.csv').put(Body=csv_buffer3.getvalue())
print('completed')