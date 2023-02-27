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
import boto3
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
from fuzzywuzzy import fuzz
from io import BytesIO
from geopy.geocoders import HereV7
from geopy import distance
import configurations as c
import openrouteservice

class Scraper():
    def save_file(link, file_name, path):
        r = requests.get(link, allow_redirects=True)
        file_path = path + file_name + '.gz'
        with open(file_path, 'wb') as fh:
            fh.write(r.content)
            print(f'wrote file {file_name}')

    def is_store(StoreId, stores_dict):
        if stores_dict.get(StoreId):
            return 1
        else:
            return 0

    def __init__(self, filepath_prefix):
        self.filepath_prefix = filepath_prefix

    def consolidate_products(chain_dict, multi_price_products_only=True):
        cons_products = {}
        chains = [chain_key for chain_key in chain_dict.keys()]
        multi_price_prod_list = []

        for chain_key, json_list in chain_dict.items():
            for product in json_list:

                if product['ItemName'] not in cons_products:
                    cons_products[product['ItemName']] = {'id': product['ItemCode'],
                                                          'name': product['ItemName'],
                                                          'chains': {s: {} for s in chains}}
                    cons_products[product['ItemName']]['chains'][chain_key][product['store_id']] = product['ItemPrice']
                else:
                    cons_products[product['ItemName']]['chains'][chain_key][product['store_id']] = product['ItemPrice']
        cons_products = list(cons_products.values())

        if multi_price_products_only:
            for product in cons_products:
                if len([chain for chain in list(product['chains'].values()) if len(chain) > 0]) > 1:
                    multi_price_prod_list.append(product)
            cons_products = multi_price_prod_list
        return cons_products

    def dict_list_to_json(dict_list, filepath_prefix, file_name):
        with open(filepath_prefix + file_name, 'w') as fh:
            json.dump(dict_list, fh, ensure_ascii=False)
            print(f'wrote {filepath_prefix + file_name} succesfully!')

    def remove_downloads(self, purged_filepath=None):
        if purged_filepath is None:
            purged_filepath = self.filepath_prefix
        purged_filepath = purged_filepath + '*'
        files = glob.glob(purged_filepath)
        for f in files:
            os.remove(f)
        print('files deleted')


class Scraper_Shufersal(Scraper):

    def __init__(self, filepath_prefix, stores):
        super().__init__(filepath_prefix)
        self.chain_id = '7290027600007'
        self.chain_name = 'Shufersal'
        self.stores = stores

    def get_stores_dict(self,
                        url='http://prices.shufersal.co.il/FileObject/UpdateCategory?catID=5&storeId=0',
                        save_file_path='/home/naya/finalproject/downloads/stores/',
                        file_name='shufersal_stores'):
        resp = requests.get(url, allow_redirects=True)
        html = resp.content.decode('utf-8')
        soup = bs(html, 'html.parser')
        link_tag = soup.find('a', attrs={'target': '_blank'})
        link = link_tag['href']
        Scraper.save_file(link, file_name, save_file_path)
        path_to_zip_file = save_file_path + file_name + '.gz'
        xml = xmltodict.parse(gzip.GzipFile(path_to_zip_file))
        stores = xml['asx:abap']['asx:values']['STORES']['STORE']
        store_dict = {store['STOREID'].zfill(3): store for store in stores}
        return store_dict

    def parse_xml_stores_file_to_json(self):
        json_list = []

        il_tz = timezone('Asia/Jerusalem')
        pulling_date = datetime.now(il_tz).strftime("%Y-%m-%d")
        stores = self.get_stores_dict()
        for store_id, store_dict in stores.items():
            if store_id in self.stores:
                store = {}
                store['StoreName'] = store_dict['STORENAME']
                store['Address'] = store_dict['ADDRESS']
                store['City'] = store_dict['CITY']
                store['store_id'] = store_id
                store['ChainId'] = self.chain_id
                store['ChainName'] = store_dict['CHAINNAME']
                store['pulling_date'] = pulling_date
                json_list.append(store)
        return json_list

    def parse_store_to_json_list(path_to_zip_file, stores_dict):
        json_list = []
        il_tz = timezone('Asia/Jerusalem')
        pulling_date = datetime.now(il_tz).strftime("%Y-%m-%d")
        xml = xmltodict.parse(gzip.GzipFile(path_to_zip_file))
        chainId = xml['root']['ChainId']
        storeId = xml['root']['StoreId'].zfill(3)
        products = xml['root']['Items']['Item']
        for product in products:
            del product['ItemType']
            del product['ManufactureCountry']
            del product['bIsWeighted']
            del product['QtyInPackage']
            del product['AllowDiscount']
            del product['ItemStatus']
            product['chain_id'] = chainId
            product['store_id'] = storeId
            product['pulling_date'] = pulling_date
            product['PriceUpdateDate'] = product['PriceUpdateDate'].split()[0]
            product['ChainName'] = stores_dict[storeId]['CHAINNAME']
            product['StoreName'] = stores_dict[storeId]['STORENAME']
            product['Address'] = stores_dict[storeId]['ADDRESS']
            product['City'] = stores_dict[storeId]['CITY']
            json_list.append(product)
        return json_list

    def scrape_stores_to_json_list(self, stores_dict):
        products = []
        initial_url = 'http://prices.shufersal.co.il/FileObject/UpdateCategory?catID=2&storeId=0&page=1'
        resp = requests.get(initial_url, allow_redirects=True)
        html = resp.content.decode('utf-8')
        soup = bs(html, 'html.parser')
        last_page_tag = soup.find('a', string=re.compile('>>'))
        last_page_num = int(last_page_tag['href'].split('=')[-1])

        if not stores_dict:
            stores_dict = self.get_stores_dict()
        for page_num in range(1, last_page_num + 1):
            url = f'http://prices.shufersal.co.il/FileObject/UpdateCategory?catID=2&storeId=0&page={page_num}'
            print(f'page num: {page_num}')
            resp = requests.get(url, allow_redirects=True)
            html = resp.content.decode('utf-8')
            soup = bs(html, 'html.parser')
            stores = soup.find_all('a', {'href': re.compile(r'PriceFull')})
            for store in stores:
                link = store['href']
                file_name = re.search(r'Price.*(?=.gz)', link).group()
                StoreId = file_name.split('-')[1]
                if self.stores is None:
                    self.stores = list(stores_dict.keys())
                if Scraper.is_store(StoreId, stores_dict) and StoreId in self.stores:
                    Scraper.save_file(link, file_name, self.filepath_prefix)
                    path_to_zip_file = self.filepath_prefix + file_name + '.gz'
                    # df = parse_store(path_to_zip_file)
                    json_list = Scraper_Shufersal.parse_store_to_json_list(path_to_zip_file, stores_dict)
                    for item in json_list:
                        products.append(item)
        return products


class Scraper_Victory(Scraper):

    def __init__(self, filepath_prefix, stores):
        super().__init__(filepath_prefix)
        self.chain_id = '7290696200003'
        self.chain_name = 'Victory'
        self.stores = stores

    def get_stores_dict(self,
                        url='http://matrixcatalog.co.il/NBCompetitionRegulations.aspx?code=7290696200003&fileType=storesfull',
                        save_file_path='/home/naya/finalproject/downloads/stores/',
                        file_name='victory_stores'):
        resp = requests.get(url, allow_redirects=True)
        html = resp.content.decode('utf-8')
        soup = bs(html, 'html.parser')
        link_tag = soup.find('a', attrs={'href': re.compile(r'StoresFull')})
        link = link_tag['href']
        link = 'http://matrixcatalog.co.il/' + link

        Scraper.save_file(link, file_name, save_file_path)
        path_to_zip_file = save_file_path + file_name + '.gz'
        xml = xmltodict.parse(gzip.GzipFile(path_to_zip_file))
        stores = xml['Store']['Branches']['Branch']
        store_dict = {store['StoreID'].zfill(3): store for store in stores}
        return store_dict

    def parse_xml_stores_file_to_json(self):
        json_list = []

        il_tz = timezone('Asia/Jerusalem')
        pulling_date = datetime.now(il_tz).strftime("%Y-%m-%d")
        stores = self.get_stores_dict()
        for store_id, store_dict in stores.items():
            if store_id in self.stores:
                store = {}
                store['StoreName'] = store_dict['StoreName']
                store['Address'] = store_dict['Address']
                store['City'] = store_dict['City']
                store['store_id'] = store_id
                store['ChainId'] = self.chain_id
                store['ChainName'] = store_dict['ChainName']
                store['pulling_date'] = pulling_date
                json_list.append(store)
        return json_list

    def parse_store_to_json_list(path_to_zip_file, stores_dict):
        json_list = []
        il_tz = timezone('Asia/Jerusalem')
        pulling_date = datetime.now(il_tz).strftime("%Y-%m-%d")

        xml = xmltodict.parse(gzip.GzipFile(path_to_zip_file))
        chainId = xml['Prices']['ChainID']
        storeId = xml['Prices']['StoreID'].zfill(3)
        products = xml['Prices']['Products']['Product']
        for product in products:
            del product['ItemType']
            del product['ManufactureCountry']
            del product['BisWeighted']
            del product['QtyInPackage']
            del product['AllowDiscount']
            del product['itemStatus']
            product['chain_id'] = chainId
            product['store_id'] = storeId
            product['pulling_date'] = pulling_date
            product['PriceUpdateDate'] = product['PriceUpdateDate'].split()[0]
            product['ChainName'] = stores_dict[storeId]['ChainName']
            product['StoreName'] = stores_dict[storeId]['StoreName']
            product['Address'] = stores_dict[storeId]['Address']
            product['City'] = stores_dict[storeId]['City']
            json_list.append(product)
        return json_list

    def scrape_stores_to_json_list(self, stores_dict):
        victory_chain_id = self.chain_id
        filetype = 'pricefull'
        products = []
        if not stores_dict:
            stores_dict = self.get_stores_dict()
        url = f'http://matrixcatalog.co.il/NBCompetitionRegulations.aspx?code={victory_chain_id}&fileType={filetype}'
        resp = requests.get(url, allow_redirects=True)
        html = resp.content.decode('utf-8')
        soup = bs(html, 'html.parser')
        stores = soup.find_all('a', {'href': re.compile(r'PriceFull')})

        for store in stores:
            link = store['href']
            link = 'http://matrixcatalog.co.il/' + link
            file_name = re.search(r'Price.*(?=.xml)', link).group()

            StoreId = file_name.split('-')[1]
            if self.stores is None:
                self.stores = list(stores_dict.keys())
            if Scraper.is_store(StoreId, stores_dict) and StoreId in self.stores:
                Scraper.save_file(link, file_name, self.filepath_prefix)
                path_to_zip_file = self.filepath_prefix + file_name + '.gz'
                # df = parse_store(path_to_zip_file)
                json_list = Scraper_Victory.parse_store_to_json_list(path_to_zip_file, stores_dict)
                for item in json_list:
                    products.append(item)
        return products


class RamiLeviScraper:
    def __init__(self):
        self.chain_name = 'Rami Levi'
        self.chain_id = '7290058140886'
        self.stores_ids = ['043', '049', '055', '057', '017', '027', '034', '038', '203', '046', '008', '014', '033',
                           '035', '050']

    def driver_settings(self):
        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--safebrowsing-disable-download-protection')
        options.add_argument('--headless')
        options.add_argument("start-maximized")
        options.binary_location = '/etc/alternatives/google-chrome'
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--allow-running-insecure-content')
        prefs = {"download.default_directory": "/home/naya/finalproject/downloads/ramilevi",
                 'safebrowsing.enabled': "false"}

        options.add_experimental_option("prefs", prefs)

        driver = webdriver.Chrome(chrome_options=options,
                                  executable_path='/home/naya/finalproject/drivers/chromedriver')
        driver.get("https://url.retail.publishedprices.co.il/login")
        return driver

    def site_login(self, driver):
        username_input = driver.find_element(By.ID, "username")
        username_input.send_keys("RamiLevi")
        login_button = driver.find_element(By.ID, "login-button").click()

    def sort_and_filter_files(self, search_term, driver):
        driver.find_element(By.XPATH, '//*[@id="fileList"]/thead/tr/th[4]').click()
        driver.find_element(By.XPATH, '//*[@id="fileList"]/thead/tr/th[4]').click()
        time.sleep(4)
        filter_input = driver.find_element(By.XPATH, '//*[@id="fileList_filter"]/input')
        filter_input.clear()
        filter_input.send_keys(search_term)
        filter_search_apply = driver.find_element(By.XPATH, '//*[@id="fileList_filter"]/span[1]/span').click()
        time.sleep(4)
        ids = driver.find_elements(by=By.XPATH, value='//*[@id]')
        time.sleep(4)
        list_of_files = [id.get_attribute('id') for id in ids if search_term in id.get_attribute('id')]
        return list_of_files

    def stores_file_download(self, driver):
        list_of_files = RamiLeviScraper.sort_and_filter_files(self, "Stores7290058140886", driver)
        file = list_of_files[0]
        driver.find_element(By.XPATH, f'//*[@id="{file}"]/td[5]/a').click()
        driver.find_element(By.NAME, 'dl-file-btn').click()
        return file

    def product_files_download(self, driver):
        list_of_files = []
        for store in self.stores_ids:
            stores_updates = RamiLeviScraper.sort_and_filter_files(self, f"PriceFull7290058140886-{store}", driver)
            last_update_file = stores_updates[0]
            driver.find_element(by=By.XPATH, value=f'//*[@id="{last_update_file}"]/td[1]/a').click()
            list_of_files.append(last_update_file)
        return list_of_files

    def parse_xml_product_files_to_json(self, list_of_product_files):
        json_products_to_kafka = []
        json_products = []
        il_tz = timezone('Asia/Jerusalem')
        pulling_date = datetime.now(il_tz).strftime("%Y-%m-%d")
        for file in list_of_product_files:
            xml = xmltodict.parse(gzip.GzipFile(f'/home/naya/finalproject/downloads/ramilevi/{file}'))
            chainId = xml['Root']['ChainId']
            store_id = xml['Root']['StoreId']
            products = xml['Root']['Items']['Item']
            for product in products:
                del product['ItemType']
                del product['ManufactureCountry']
                del product['bIsWeighted']
                del product['QtyInPackage']
                del product['AllowDiscount']
                del product['ItemStatus']
                del product['ItemId']
                product["store_id"] = store_id.zfill(3)
                product['pulling_date'] = pulling_date
                product['PriceUpdateDate'] = product['PriceUpdateDate'].split()[0]
                json_products.append(product)
        ##for item in json_products:
        ##    json_products_to_kafka.append(json.dumps(item, ensure_ascii=False))
        return json_products
        ##return json_products_to_kafka

    def parse_xml_stores_file_to_json(self, stores_file):
        json_stores_to_kafka = []
        json_stores = []
        il_tz = timezone('Asia/Jerusalem')
        pulling_date = datetime.now(il_tz).strftime("%Y-%m-%d")
        with open(f'/home/naya/finalproject/downloads/ramilevi/{stores_file}', encoding='utf16') as fd:
            xml = xmltodict.parse(fd.read())
        chainId = xml['Root']['ChainId']
        ChainName = xml['Root']['ChainName']
        stores = xml['Root']['SubChains']['SubChain']['Stores']['Store']
        for store in stores:
            if store["StoreId"].zfill(3) in self.stores_ids:
                del store['ZipCode']
                del store['StoreType']
                del store['BikoretNo']
                store["store_id"] = store["StoreId"].zfill(3)
                store["ChainId"] = chainId
                store["ChainName"] = ChainName
                store['pulling_date'] = pulling_date
                del store['StoreId']
                json_stores.append(store)
            else:
                pass
        ##for item in json_stores:
        ##    json_stores_to_kafka.append(json.dumps(item, ensure_ascii=False))
        return json_stores
        ##return json_products_to_kafka


def dict_key_value_count(item, key):
    return len([value for value in list(item[key].values()) if len(value) > 0])


def most_freq_key(dic):
    return [key for key, value in dic.items() if len(value) > 0][0]


def merge_dicts(x):
    return {k: v for d in x for k, v in d.items() if len(v) > 0}

def send_to_s3(object, bucket, bucket_path, object_name):

    s3 = boto3.client("s3", \
                        region_name=c.aws_region, \
                        aws_access_key_id=c.aws_access_key, \
                        aws_secret_access_key=c.aws_secret_key)

    s3.put_object(Bucket=bucket,\
                Body=object,\
                Key=f'{bucket_path}{object_name}')

def parquet_to_s3(object, bucket, path):

    s3 = boto3.client("s3", \
                        region_name=c.aws_region, \
                        aws_access_key_id=c.aws_access_key, \
                        aws_secret_access_key=c.aws_secret_key)
    out_buffer = BytesIO()
    df = pd.DataFrame(object)
    df.to_parquet(out_buffer, index=False)
    s3.put_object(Bucket=c.aws_bucket, Key=path, Body=out_buffer.getvalue())

def geocode_p(address):
    """
    example: 
        location = geocode_p('אחד העם 17, פתח תקווה')
        print(location.latitude, location.longitude)
    
    """
    geolocator = HereV7(apikey=c.geocode_api_key)
    location = geolocator.geocode(address)
    return location

def get_distance(lat1, lon1, lat2, lon2):
    client = openrouteservice.Client(key=c.distance_api_key)
    coords = ((lon1, lat1), (lon2, lat2))
    route = client.directions(coords)
    return route.get('routes')[0].get('summary').get('distance')

if __name__ == '__main__':
    scraper_lev = RamiLeviScraper()
    driver = scraper_lev.driver_settings()
    scraper_lev.site_login(driver)
    stores_file = scraper_lev.stores_file_download(driver)
    time.sleep(1)
    list_of_product_files = scraper_lev.product_files_download(driver)
    time.sleep(5)
    driver.close()
    print("Downloads Finished.")
    json_products = scraper_lev.parse_xml_product_files_to_json(list_of_product_files)
    json_stores = scraper_lev.parse_xml_stores_file_to_json(stores_file)
    print("Parsing Finished.")

    ##===============Shufersal==========================##
    shufersal_stores = ['001', '035', '106', '109', '129', '169', '216', '224', '269', '312', '361', '362', '374',
                        '477', '780']

    scraper_shu = Scraper_Shufersal('/home/naya/finalproject/downloads/shufersal/', stores=shufersal_stores)

    stores_dict_shu = scraper_shu.get_stores_dict()

    json_list_shu = scraper_shu.scrape_stores_to_json_list(stores_dict_shu)

    ##===============Victory============================##
    victory_stores = ['002', '008', '014', '022', '027', '041', '046', '051', '059', '061', '068', '073', '074', '081',
                      '083']

    scraper_vic = Scraper_Victory('/home/naya/finalproject/downloads/victory/', stores=victory_stores)

    stores_dict_vic = scraper_vic.get_stores_dict()

    json_list_vic = scraper_vic.scrape_stores_to_json_list(stores_dict_vic)

    # with open('./json_list_shu.pkl', 'rb') as fh:
    #    shu_list = pickle.load(fh)

    # with open('./json_list_vic.pkl', 'rb') as fh:
    #    vic_list = pickle.load(fh)

    cons_prod = Scraper.consolidate_products(
        {'victory': json_list_vic, 'shufersal': json_list_shu, 'ramilevi': json_products},
        multi_price_products_only=False)

    singles = [product for product in cons_prod if dict_key_value_count(product, 'chains') == 1]

    rest = [product for product in cons_prod if dict_key_value_count(product, 'chains') > 1]

    for product in rest.copy():
        for chain, value in product['chains'].copy().items():
            if len(value) == 0:
                del product['chains'][chain]

    ##remove duplicate names
    for product in singles:
        product['cname'] = most_freq_key(product['chains'])

    df = pd.DataFrame(singles).drop_duplicates(subset=['name', 'cname'], keep='first')

    ##merge products from different chains by id
    df_gb = df.groupby('id', as_index=False).agg({'name': 'min', 'chains': merge_dicts})[['id', 'name', 'chains']]
    singles_no_dup = df_gb.to_dict('records')

    product_list = rest + singles_no_dup

    scraper_shu.remove_downloads()
    scraper_vic.remove_downloads()
    scraper_vic.remove_downloads(purged_filepath='/home/naya/finalproject/downloads/ramilevi/')
