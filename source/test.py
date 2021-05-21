"""
Create the database


import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="qqqq1234"
)

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE future_sale")
"""

import requests

BASE = 'http://127.0.0.1:5000/'

# res1 = requests.put(BASE + 'item_categories', {'csv': '../dataset/item_categories.csv'})
# res2 = requests.put(BASE + 'items', {'csv': '../dataset/items.csv'})
# res3 = requests.put(BASE + 'shops', {'csv': '../dataset/shops.csv'})
res4 = requests.put(BASE + 'sales', {'csv': '../dataset/sales_train.csv'})

# print(res1.json())
# print(res2.json())
# print(res3.json())
print(res4.json())

# res5 = requests.get(BASE + 'item_categories', {})
# print(res5.json())