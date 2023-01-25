import pymysql, os
from dotenv import load_dotenv

#---Loading .env Variables--- 
load_dotenv()

#---Establishing Database Connection---
connection = pymysql.connect(
    host = os.getenv("mysql_host"),
    user = os.getenv("mysql_user"),
    password = os.getenv("mysql_pass"),
    database = os.getenv("mysql_db")
)

cursor = connection.cursor()

try:
    cursor.execute('SELECT * FROM All_Data')
    cursor.execute('SELECT * FROM Order_Info')
    cursor.execute('SELECT * FROM Product_Info')
except:
    with open('./src/group_project_db.sql', 'r') as file:
        sqlFile = file.read()
        sqlCommands = sqlFile.split(';')
        for command in sqlCommands:
            cursor.execute(command)
    file.close()

#---Exporting Data Frame into SQL Database---
from app import main_table, orders_table, products_table

#---Exporting Data Frame into SQL Database---
for x in orders_table.values.tolist():
    cursor.execute(f"INSERT INTO All_Data (timestamp, store_name, total_price, payment_method) VALUES {tuple(x)}")
    connection.commit()

#---Exporting Product Information into SQL Database---
for x in products_table.values.tolist():
    cursor.execute(f"INSERT INTO Product_Info (name, flavour, price) VALUES {tuple(x)}")
    connection.commit()

#---Exporting Order Information into SQL Database---
for x in main_table.values.tolist():
    cursor.execute(f"INSERT INTO Order_Info (order_id, product_id, quantity, price) VALUES {tuple(x)}")
    connection.commit()

cursor.close()
connection.close()