from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import csv
import time

urls = ["https://www.amazon.eg/s?i=electronics&rh=n%3A18018102031%2Cn%3A21832872031&s=popularity-rank&dc&fs=true&language=en&ds=v1%3AvNguSJEWGicgApD7xI1GZy9Zw4BlH5TMtCO2ei4INsg&qid=1763928697&rnid=18018102031&xpid=9pArP_2fBJc6P&ref=sr_nr_n_3",
        "https://www.amazon.eg/s?i=electronics&rh=n%3A18018102031%2Cn%3A21832868031&s=popularity-rank&dc&fs=true&language=en&ds=v1%3AxjPlHzyUy2ph%2BS8mZvtjv%2B6T7z0sfJO2Wb0VUfTQO9I&qid=1763928697&rnid=18018102031&xpid=9pArP_2fBJc6P&ref=sr_nr_n_9",
        "https://www.amazon.eg/s?i=home&rh=n%3A18021933031%2Cn%3A21863794031&s=popularity-rank&dc&fs=true&language=en&ds=v1%3A85BXSgHJ5QVT3Ic%2BLSYJJU9SI%2B1anGacdJX4nXsFE%2Bk&qid=1763929189&rnid=18021933031&ref=sr_nr_n_8",
        "https://www.amazon.eg/s?i=videogames&rh=n%3A18022560031&s=popularity-rank&fs=true&language=en&ref=lp_18022560031_sar",
        "https://www.amazon.eg/s?i=home&rh=n%3A21863792031&s=popularity-rank&fs=true&language=en&ref=lp_21863792031_sar",
        ]

product_details = []
categories = ["computers-and-accessories", "mobiles-and-accessories", "kitchen-utensils-and-gadgets",
              "video-games", "home-appliances"]

def amazon(urls, categories):
    
    service = Service(ChromeDriverManager().install())
    browser = webdriver.Chrome(service=service)
    
    for i, url in enumerate(urls):
        page_num = 1

        while True:
            try:
                full_url = f"{url}&page={page_num}"
                print(f"🔍 Scraping: {full_url}")
                
                browser.get(full_url)
                time.sleep(3)

                # Parse page HTML
                soup = BeautifulSoup(browser.page_source, "html.parser")

                # Get products
                items = soup.find_all("div", {"class": "s-result-item"})
                
                if not items:
                    print("⚠ No more products → stopping.")
                    break

                for p in items:
                    try:
                        name = p.find("h2", {"class": "a-text-normal"}).text.strip()
                    except:
                        name = "Not found"
                    try:
                        price = p.find("span", {"class": "a-price-whole"}).text.strip()
                    except:
                        price = "Not found"
                    try:
                        old_price = p.find('div', {'class':'aok-inline-block'}).text
                    except:
                        old_price = "No discount"
                    try:
                        product_rate = p.find('span', {'a-icon-alt'}).text
                    except:
                        product_rate = "Rate not found"
                    try:
                        link = "https://www.amazon.eg" + p.find('a', {"class":"a-link-normal"}).get('href')
                    except:
                        link = "Not found"

                    product_details.append({
                        "Product Name":name,
                        "Price":price,
                        "Old Price":old_price,
                        "Rate":product_rate,
                        "Category":categories[i],
                        "Company": "Noon",
                        "Product Page":link
                    })
                
                # Check if exist any pages
                if page_num >= 150:
                    print("Pages Ended")
                    break

                page_num += 1
            except Exception as e:
                print(f"❌ Error: {e}")
                break

    return product_details

def create_file():
    path = r'amazon_products.csv' 
    keys = product_details[0].keys()
    with open(path, 'w', newline='', encoding='UTF-8') as output:
        dict_writer = csv.DictWriter(output, keys)
        dict_writer.writeheader()
        dict_writer.writerows(product_details)
    print("File created successfully")    
    
amazon(urls, categories)
create_file()