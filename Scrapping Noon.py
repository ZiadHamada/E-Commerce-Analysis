from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
from bs4 import BeautifulSoup
import csv

urls = ["https://www.noon.com/egypt-ar/electronics-and-mobiles/computers-and-accessories/",
        "https://www.noon.com/egypt-ar/electronics-and-mobiles/mobiles-and-accessories/",
        "https://www.noon.com/egypt-ar/electronics-and-mobiles/video-games-10181/",
        "https://www.noon.com/egypt-ar/home-and-kitchen/home-appliances-31235/",
        "https://www.noon.com/egypt-ar/home-and-kitchen/kitchen-and-dining/kitchen-utensils-and-gadgets/"]

product_details = []
categories = ["computers-and-accessories", "mobiles-and-accessories", "video-games", "home-appliances", 
              "kitchen-utensils-and-gadgets"]

def noon(urls, categories):

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
                items = soup.find_all("div", {"class": "PBoxLinkHandler-module-scss-module__WvRpgq__linkWrapper"})
                
                if not items:
                    print("⚠ No more products → stopping.")
                    break

                for p in items:
                    try:
                        name = p.find("h2", {"class": "ProductDetailsSection-module-scss-module__Y6u1Qq__title"}).text.strip()
                    except:
                        name = "Not found"

                    try:
                        price = p.find("strong", {"class": "Price-module-scss-module__q-4KEG__amount"}).text.strip()
                    except:
                        price = "Not found"
                    try:
                        product_discount = p.find('span', {'class':'PriceDiscount-module-scss-module__6h-Fca__pBox'}).text
                    except:
                        product_discount = "No discount"
                    try:
                        product_rate = p.find('div', {'RatingPreviewStar-module-scss-module__zCpaOG__textCtr'}).text
                    except:
                        product_rate = "Rate not found"
                    try:
                        link = "https://www.noon.com" + p.find('a').get('href')
                    except:
                        link = "Not found"

                    product_details.append({
                        "Product Name":name,
                        "Price":price,
                        "Discount":product_discount,
                        "Rate":product_rate,
                        "Category":categories[i],
                        "Company": "Noon",
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

    browser.quit()
    return product_details

def create_file():
    path = r'noon_products.csv' 
    keys = product_details[0].keys()
    with open(path, 'w', newline='', encoding='UTF-8') as output:
        dict_writer = csv.DictWriter(output, keys)
        dict_writer.writeheader()
        dict_writer.writerows(product_details)
    print("File created successfully")    
    
noon(urls, categories)
create_file()