import time
import requests
import json
import pandas as pd
import gzip
import xmltodict
import re
import pickle
import os
import glob
import sys
sys.path.append('..')
from kafka import KafkaProducer
from bs4 import BeautifulSoup as bs
from pytz import timezone
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from kafka import KafkaProducer
from scrape_class import Scraper, Scraper_Shufersal, Scraper_Victory, RamiLeviScraper, dict_key_value_count, merge_dicts, most_freq_key, send_to_s3
import Configuration as c
from elasticsearch import Elasticsearch

il_tz = timezone('Asia/Jerusalem')
pulling_date = datetime.now(il_tz).strftime("%Y-%m-%d")

##===============RamiLevi==========================##
# with open('/home/naya/finalproject/code/rl_products.json', 'r') as fh:
#     json_list_lev = json.load(fh)

scraper_lev = RamiLeviScraper()
driver = scraper_lev.driver_settings()
scraper_lev.site_login(driver)

time.sleep(5)
list_of_product_files = scraper_lev.product_files_download(driver)
print('rami levi finished download files')
time.sleep(1)
json_products_lev = scraper_lev.parse_xml_product_files_to_json(list_of_product_files)
print('rami levi finished parsing products')
driver.close()

##===============Shufersal==========================##
shufersal_stores = c.shufersal_stores

scraper_shu = Scraper_Shufersal(c.shufersal_download_path, stores=shufersal_stores)
stores_dict_shu = scraper_shu.get_stores_dict()
json_list_shu = scraper_shu.scrape_stores_to_json_list(stores_dict_shu)


##===============Victory============================##
victory_stores = c.victory_stores

scraper_vic = Scraper_Victory(c.victory_download_path,stores=victory_stores)
stores_dict_vic = scraper_vic.get_stores_dict()
json_list_vic = scraper_vic.scrape_stores_to_json_list(stores_dict_vic)



##===============Join the Products=================##
cons_prod = Scraper.consolidate_products({'victory':json_list_vic, 'shufersal':json_list_shu, 'ramilevi':json_products_lev}, multi_price_products_only=False)
singles = [product for product in cons_prod if dict_key_value_count(product, 'chains')==1]
rest = [product for product in cons_prod if dict_key_value_count(product, 'chains') > 1]
for product in rest.copy():
    for chain, value in product['chains'].copy().items():
        if len(value) == 0:
            del product['chains'][chain]
##remove duplicate names in products
for product in singles:
    product['cname'] = most_freq_key(product['chains'])
df = pd.DataFrame(singles).drop_duplicates(subset=['name','cname'], keep='first')
##merge products from different chains by id
df_gb = df.groupby('id', as_index=False).agg({'name':'min','chains':merge_dicts})[['id', 'name', 'chains']]
singles_no_dup = df_gb.to_dict('records')
product_list =rest + singles_no_dup

for product in product_list:
    product['pulling_date'] = pulling_date
    product['chains_count'] = len(product['chains'])
    product['stores_count'] = sum([len(stores) for chain, stores in product['chains'].items()])

#delete the current idx
es = Elasticsearch(hosts="http://localhost:9200")
indexes = es.indices.get_alias('*')
if c.products_idx in indexes:
    es.indices.delete(index=c.products_idx)


# this load is just for quick testing
# with open('/home/naya/finalproject/code/product_lst.json', 'rb') as fh:
#     product_list = json.load(fh)

producer = KafkaProducer(bootstrap_servers=c.brokers)

for row in product_list:
    producer.send(topic=c.products_topic, value=json.dumps(row).encode('utf-8'))
    producer.flush()


# #get current date
# il_tz = timezone('Asia/Jerusalem')
# pulling_date = datetime.now(il_tz).strftime("%Y-%m-%d")

# #load to s3 bucket
json_object = json.dumps(product_list, ensure_ascii=False)

send_to_s3(json_object, c.aws_bucket, c.aws_products_path, object_name=pulling_date+'.json')


#remove downloaded files
scraper_shu.remove_downloads()
scraper_vic.remove_downloads()
scraper_vic.remove_downloads(purged_filepath='/home/naya/finalproject/downloads/ramilevi/')