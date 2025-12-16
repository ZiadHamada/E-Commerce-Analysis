# -*- coding: utf-8 -*-
"""
Created on Mon Nov 24 05:38:16 2025

@author: HP
"""
import pandas as pd
import csv
import re

def clean_product_names(df_names):
    return [name.strip() for name in df_names]

def clean_rate(df_rates):
    rates = []
    for rate in df_rates:
        try:
            rates.append(list(map(float, re.findall(r'\d+.\d+', rate)))[0])
        except:
            rates.append(0)
    return rates

def clean_price(df_prices):
    prices = []
    for price in df_prices:
        try:
            prices.append(float((re.findall(r'\d+.\d+', price.replace(",", "")))[0]))
        except:
            prices.append(0)
    return prices

def clean_old_price(df_old_prices):
    old_prices = []
    for old_price in df_old_prices:
        old_price = old_price.replace(",", "")
        old_price = re.findall(r'\d+', old_price)
        try:
            old_prices.append(list(map(float, old_price))[0])
        except:    
            old_prices.append(0)
    return old_prices

def clean_discount(df_discounts):
    discounts = []
    for disc in df_discounts:
        try:
            discounts.append(list(map(int, re.findall(r'\d+', disc)))[0])
        except:
            discounts.append(0) 
    return discounts

# TOTAL_COLS = 7   # <-- change this number if your CSV has more or fewer columns
# TRAILING = TOTAL_COLS - 1   # all columns except Product Name

# rows = []
# with open("amazon_products.csv", "r", encoding="windows-1256", errors="ignore") as f:
#     for line in f:
#         parts = line.rstrip("\n").split(",")

#         # Skip empty lines
#         if len(parts) == 0:
#             continue

#         # Last N columns are fixed
#         tail = parts[-TRAILING:]
#         name = ",".join(parts[:-TRAILING])

#         rows.append([name] + tail)

# # First row is header
# header = rows[0]
# data = rows[1:]
#amazon_df = pd.DataFrame(data, columns=header)
#amazon_df = pd.read_csv("amazon_products.csv", encoding='windows-1256', engine="python", quotechar='"', escapechar='\\', on_bad_lines="skip")
amazon_df = pd.read_csv("amazon_products.csv", encoding='windows-1256')
jumia_df = pd.read_csv("jumia_products.csv")
noon_df = pd.read_csv("noon_products.csv")

print(len(amazon_df))
print(len(noon_df))
print(len(jumia_df))

##########################################################################
##################################amazon_df###############################
##########################################################################
amazon_df["Product Name"] = [name.replace(",","").strip() for name in amazon_df["Product Name"]]

amazon_df["Price"] = clean_price(amazon_df["Price"])

amazon_df["Old Price"] = clean_old_price(amazon_df["Old Price"])

amazon_df["Discount"] = ((amazon_df["Old Price"] - amazon_df["Price"]) / amazon_df["Old Price"]) * 100
amazon_df["Discount"] = pd.to_numeric(amazon_df["Discount"], errors="coerce")
amazon_df["Discount"].replace([float("inf"), -float("inf")], 0, inplace=True)
amazon_df["Discount"] = amazon_df["Discount"].fillna(0).astype(int)

amazon_df.drop(columns=["Old Price"], inplace = True)

amazon_df["Rate"] = clean_rate(amazon_df["Rate"])

amazon_df = amazon_df.reindex(['Product Name', 'Price', 'Discount', 'Rate', 'Category', 'Company', 'Product Page'], axis=1)

#########################################################################
##################################jumia_df###############################
#########################################################################

jumia_df["Product Name"] = [name.replace(",","").strip() for name in jumia_df["Product Name"]]

jumia_df["Price"] = clean_price(jumia_df["Price"])

jumia_df["Discount"] = clean_discount(jumia_df["Discount"])

jumia_df["Rate"] = clean_rate(jumia_df["Rate"])
   
#########################################################################
##################################noon_df###############################
#########################################################################

noon_df["Product Name"] = [name.replace(",","").strip() for name in noon_df["Product Name"]]

noon_df["Price"] = clean_price(noon_df["Price"])

noon_df["Discount"] = clean_discount(noon_df["Discount"])

noon_df["Rate"] = clean_rate(noon_df["Rate"])
   
amazon_df.drop_duplicates(inplace=True)
jumia_df.drop_duplicates(inplace=True)
noon_df.drop_duplicates(inplace=True)

amazon_df = amazon_df[amazon_df['Price'] > 0]
jumia_df = jumia_df[jumia_df['Price'] > 0]
noon_df = noon_df[noon_df['Price'] > 0]

amazon_df.insert(loc=0, column="Product ID", value=[i+1 for i in range(len(amazon_df))])
jumia_df.insert(loc=0, column="Product ID", value=[(i + 20000) for i in range(len(jumia_df))])
noon_df.insert(loc=0, column="Product ID", value=[(i + 40000) for i in range(len(noon_df))])

dfs = [amazon_df, jumia_df, noon_df]
for df in dfs:
    print(df.columns)
    for col in df.columns:
        print(type(df[col][0]))
        
for df in dfs:
    print(df.iloc[500, :])    
    
print(len(amazon_df))
print(len(jumia_df))
print(len(noon_df))

amazon_df.to_csv(r"D:\Data Analysis\E-Commerce Analytics\Datasets\amazon_products.csv", index=False, encoding="utf-8-sig")
jumia_df.to_csv(r"D:\Data Analysis\E-Commerce Analytics\Datasets\jumia_products.csv", index=False, encoding="utf-8-sig")
noon_df.to_csv(r"D:\Data Analysis\E-Commerce Analytics\Datasets\noon_products.csv", index=False, encoding="utf-8-sig")