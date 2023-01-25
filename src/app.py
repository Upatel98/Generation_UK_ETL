import os
import pandas as pd
import Product

dirname ='../datasets'
#---Iterates over file and finds files that have csv extensions then store in a list of files---
def iterate_files(name):
    dirname=name
    ext=('.csv')
    df_list = []
    for file in os.listdir(dirname):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            name = "D:\\anything\\datasets\\" + filename
            df_list.append(pd.read_csv(name,names=["Timestamp", "Store name", "Customername", "Basket id", "total price","Payment method", "Cardnumber"]))
    return df_list

final_df =pd.concat(iterate_files(dirname), ignore_index=True)

#---Temporary function to remove sensitive information from table---
def drop_sensitive(df):
    return df.drop(["Customername", "Cardnumber"], axis=1)

test_df = drop_sensitive(final_df)
test_df = test_df.drop_duplicates()

#---Function to create an index for the list of products sold---
def index_list_maker(df):
    test_df_products = df["Basket id"].tolist()
    new_list = []
    for i in test_df_products:
        new_list.append(i.split(","))
    final_list = Product.drinks_list
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

products_table = pd.DataFrame(Product.drinks_list, columns=['product_name'])

#---Function to replace basket items with its equivelent key on the products list---
def replace_keys(df, index):
    index_list = index(df)
    for i in range(len(df["Basket id"])):
        df["Basket id"].iloc[i] = ' '.join(str(x) for x in index_list[i])
    return df

df = replace_keys(test_df,index_list_maker)

#---Function to seperate the list items into a table---
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