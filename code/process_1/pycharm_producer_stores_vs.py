import time
import requests
import json
import pandas as pd
import gzip
import xmltodict
import re
import os
import boto3
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
from scrape_class import Scraper, Scraper_Shufersal, Scraper_Victory, RamiLeviScraper, send_to_s3, geocode_p
import Configuration as c
from elasticsearch import Elasticsearch




##===============Shufersal==========================##
shufersal_stores = c.shufersal_stores

scraper_shu = Scraper_Shufersal(c.shufersal_download_path, stores=shufersal_stores)

json_stores_shu = scraper_shu.parse_xml_stores_file_to_json()

##===============Victory============================##
victory_stores = c.victory_stores

scraper_vic = Scraper_Victory(c.victory_download_path,stores=victory_stores)

json_stores_vic = scraper_vic.parse_xml_stores_file_to_json()

##===============Rami Levi============================##
scraper_lev = RamiLeviScraper()
driver = scraper_lev.driver_settings()
scraper_lev.site_login(driver)
time.sleep(1)
stores_file = scraper_lev.stores_file_download(driver)
time.sleep(1)
json_stores_lev = scraper_lev.parse_xml_stores_file_to_json(stores_file)

##=============send data via kafka=====================##
json_stores = json_stores_shu + json_stores_vic + json_stores_lev

#Enrich data set add latitude and longitude
for store in json_stores:
    location = geocode_p(store['Address']+', '+store['City'])
    store['Latitude'] = location.latitude
    store['Longitude'] = location.longitude


##Delete the last stores index

es = Elasticsearch(hosts="http://localhost:9200")
indexes = es.indices.get_alias('*')
if c.stores_idx in indexes:
    es.indices.delete(index=c.stores_idx)
    
# with open('./rl_stores.json', 'r') as fh:
#     data = json.load(fh)


# producer = KafkaProducer(
# 	bootstrap_servers=c.brokers,
# 	client_id='producer',
# 	acks=1,
# 	compression_type=None,
# 	retries=3,
# 	reconnect_backoff_ms=50,
# 	reconnect_backoff_max_ms=1000
# 	#,value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

producer = KafkaProducer(bootstrap_servers=c.brokers)

for row in json_stores:
    print(row)
    time.sleep(1)
    producer.send(topic=c.stores_topic, value=json.dumps(row).encode('utf-8'))
    producer.flush()

#get current date
il_tz = timezone('Asia/Jerusalem')
pulling_date = datetime.now(il_tz).strftime("%Y-%m-%d")

#load to s3 bucket
json_object = json.dumps(json_stores, ensure_ascii=False)

send_to_s3(json_object, c.aws_bucket, c.aws_stores_path, object_name=pulling_date+'.json')

