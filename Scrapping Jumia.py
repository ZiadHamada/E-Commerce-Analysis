from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import csv
import time

urls = ["https://www.jumia.com.eg/ar/phones-tablets/",
        "https://www.jumia.com.eg/ar/computing/",
        "https://www.jumia.com.eg/ar/mlp-category-appliances/",
        "https://www.jumia.com.eg/ar/video-games/",
        "https://www.jumia.com.eg/ar/home-office/",
        ]

product_details = []
categories = ["mobiles-and-accessories", "computers-and-accessories", "kitchen-utensils-and-gadgets",
              "video-games", "home-appliances"]

def amazon(urls, categories):
    
    service = Service(ChromeDriverManager().install())
    browser = webdriver.Chrome(service=service)
    
    for i, url in enumerate(urls):
        page_num = 1

        while True:
            try:
                full_url = f"{url}?page={page_num}"
                print(f"🔍 Scraping: {full_url}")
                
                browser.get(full_url)
                time.sleep(3)

                # Parse page HTML
                soup = BeautifulSoup(browser.page_source, "html.parser")

                # Get products
                items = soup.find_all("article", {"class": "c-prd"})
                
                if not items:
                    print("⚠ No more products → stopping.")
                    break

                for p in items:
                    try:
                        name = p.find("h3", {"class": "name"}).text.strip()
                    except:
                        name = "Not found"
                    try:
                        price = p.find("div", {"class": "prc"}).text.strip()
                    except:
                        price = "Not found"
                    try:
                        discount = p.find('div', {'class':'_dsct'}).text
                    except:
                        discount = "No discount"
                    try:
                        product_rate = p.find('div', {'stars'}).text
                    except:
                        product_rate = "Rate not found"
                    try:
                        link = "https://www.jumia.com.eg" + p.find('a', {"class":"core"}).get('href')
                    except:
                        link = "Not found"

                    product_details.append({
                        "Product Name":name,
                        "Price":price,
                        "Discount":discount,
                        "Rate":product_rate,
                        "Category":categories[i],
                        "Company": "Jumia",
                        "Product Page":link
                    })
                
                # Check if exist any pages
                if page_num >= 50:
                    print("Pages Ended")
                    break

                page_num += 1
            except Exception as e:
                print(f"❌ Error: {e}")
                break

    return product_details

def create_file():
    path = r'jumia_products.csv' 
    keys = product_details[0].keys()
    with open(path, 'w', newline='', encoding='UTF-8') as output:
        dict_writer = csv.DictWriter(output, keys)
        dict_writer.writeheader()
        dict_writer.writerows(product_details)
    print("File created successfully")    
    
amazon(urls, categories)
create_file()