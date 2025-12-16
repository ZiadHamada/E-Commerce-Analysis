# -*- coding: utf-8 -*-
"""
Created on Mon Nov 24 03:24:50 2025

@author: HP
"""

from faker import Faker
import pandas as pd
import random
from datetime import timedelta

# ==========================
# LOAD PRODUCT DATA
# ==========================
datasets = ["amazon_products", "jumia_products", "noon_products"]
companies = ["Amazon", "Jumia", "Noon"]
paths = ["amazon_fake_data",
         "jumia_fake_data",
         "noon_fake_data"]

customer_id = 100000
order_id_counter = 10000
order_item_id_counter = 30000
shipping_id_counter = 50000
return_id_counter = 70000

egypt_cities = [
    "القاهرة", "الجيزة", "الإسكندرية", "الشرقية", "الدقهلية", "المنوفية",
    "القليوبية", "الغربية", "البحيرة", "كفر الشيخ", "دمياط", "بورسعيد",
    "الإسماعيلية", "السويس", "الفيوم", "بني سويف", "المنيا", "أسيوط",
    "سوهاج", "قنا", "الأقصر", "أسوان", "البحر الأحمر", "مطروح",
    "شمال سيناء", "جنوب سيناء", "الوادي الجديد"
]

for i in range(len(datasets)):
    products_df = pd.read_csv(f"Datasets\{datasets[i]}.csv")
    
    # ==========================
    # SETUP ARABIC FAKER
    # ==========================
    fake = Faker("ar_EG")
    
    # ==========================
    # 1️⃣ GENERATE CUSTOMERS
    # ==========================
    customers = []
    num_customers = 50000
    
    for _ in range(num_customers):
        customers.append({
            "Customer ID": customer_id,
            "Customer Name": fake.name(),
            "Phone": fake.phone_number(),
            "City": random.choice(egypt_cities),
            "Age": random.randint(18, 60),
            "Gender": random.choice(["ذكر", "أنثى"])
        })
        customer_id += 1
    
    customers_df = pd.DataFrame(customers)
    
    # ==========================
    # 2️⃣ GENERATE ORDERS
    # ==========================
    orders = []
    order_items = []
    shipping = []
    returns = []
    
    payment_methods = ["كاش", "فيزا", "ماستر كارد", "عند الاستلام"]
    shipping_statuses = ["قيد التجهيز", "تم الشحن", "في الطريق", "تم التسليم"]
    couriers = ["أرامكس", "دي إتش إل", "فيديكس", "سمسا", "البريد المصري"]
    return_reasons = ["منتج تالف" ,"لا يعمل", "وصل غير المطلوب", "تراجع عن الشراء"]
    return_statuses = ["مقبول", "مرفوض", "تحت المراجعة"]
    
    for _, customer in customers_df.iterrows():
    
        num_orders = random.randint(2, 5)
    
        for _ in range(num_orders):
    
            order_id = order_id_counter
            order_id_counter += 1
    
            order_date = fake.date_between(start_date='-2y', end_date='today')
    
            # Save order
            orders.append({
                "Order ID": order_id,
                "Customer ID": customer["Customer ID"],
                "Order Date": order_date,
                "Payment Method": random.choice(payment_methods),
                "City": customer["City"]
            })
    
            # ==========================
            # 3️⃣ ORDER ITEMS
            # ==========================
            num_items = random.randint(1, 3)
    
            for _ in range(num_items):
                product = products_df.sample(1).iloc[0]
    
                quantity = random.randint(1, 4)
                price = product["Price"]
                total_price = price * quantity
    
                order_item_id = order_item_id_counter
                order_item_id_counter += 1
    
                order_items.append({
                    "Order Item ID": order_item_id,
                    "Order ID": order_id,
                    "Product ID": product["Product ID"],
                    "Product Name": product["Product Name"],
                    "Category": product["Category"],
                    "Company": product["Company"],
                    "Quantity": quantity,
                    "Price": price,
                    "Total Price": total_price,
                })
    
                # ==========================
                # 4️⃣ RETURNS & REFUNDS (10–15% chance)
                # ==========================
                if random.random() < 0.12:  # 12% probability
                    refund_amount = round(total_price * random.uniform(0.6, 1), 2)
    
                    returns.append({
                        "Return ID": return_id_counter,
                        "Order Item ID": order_item_id,
                        "Customer ID": customer["Customer ID"],
                        "Return Date": order_date + timedelta(days=random.randint(2, 20)),
                        "Reason": random.choice(return_reasons),
                        "Refund Amount": refund_amount
                    })
    
                    return_id_counter += 1
    
            # ==========================
            # 5️⃣ SHIPPING TABLE
            # ==========================
            shipped_date = order_date + timedelta(days=random.randint(1, 3))
            delivery_date = shipped_date + timedelta(days=random.randint(2, 7))
    
            shipping.append({
                "Shipment ID": shipping_id_counter,
                "Order ID": order_id,
                "Customer ID": customer["Customer ID"],
                "Tracking Number": fake.bothify(text="TRK########"),
                "Courier": random.choice(couriers),
                "Shipping Status": random.choice(shipping_statuses),
                "Shipped Date": shipped_date,
                "Delivery Date": delivery_date
            })
    
            shipping_id_counter += 1
    
    
    # ==========================
    # SAVE TO CSV FILES
    # ==========================
    customers_df.to_csv(f"Datasets\{paths[i]}\{companies[i]}_customers.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(orders).to_csv(f"Datasets\{paths[i]}\{companies[i]}_orders.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(order_items).to_csv(f"Datasets\{paths[i]}\{companies[i]}_order_items.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(shipping).to_csv(f"Datasets\{paths[i]}\{companies[i]}_shipping.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(returns).to_csv(f"Datasets\{paths[i]}\{companies[i]}_returns.csv", index=False, encoding="utf-8-sig")
    
    print("✅ Finished generating all dataset files:")
    print(f"{companies[i]} customers.csv")
    print(f"{companies[i]} orders.csv")
    print(f"{companies[i]} order_items.csv")
    print(f"{companies[i]} shipping.csv")
    print(f"{companies[i]} returns.csv")
    
    order_id_counter += 50000
    order_item_id_counter += 50000
    shipping_id_counter += 50000
    return_id_counter += 50000
