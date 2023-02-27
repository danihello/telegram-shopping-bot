import pandas as pd
from kafka import KafkaProducer
import json
import telebot
import Configuration as c
from elasticsearch import Elasticsearch
from geo_codes import geocode_p, get_distance
from Top5productSpark import calc_top_5

bot = telebot.TeleBot(c.TOKEN)
es = Elasticsearch(hosts="http://localhost:9200")

pd.set_option('display.max_columns', None)
pd.set_option('expand_frame_repr', False)

# Data Frame Enrichment
def enrich_df(df, dict):
    new_dict = pd.DataFrame([dict])
    df1 = pd.concat([df, new_dict])
    return df1

def add_retailer_names(dict, retailer_name):
    if not pd.isna(dict):
        result = {key+'@'+retailer_name: value for key, value in dict.items()}
        return result
    else:
        return {}

def product_to_list(dict_to_print):
    text ='המוצרים בעגלה הם: '
    product_list = [text]
    for product in dict_to_print:
        product_list.append(f"{product['name']} {product['product_quantity']}")
    return '\n'.join(product_list)

# get data from elastic
def get_product_date_from_elastic(product_name, product_quantity):
    print("now being processed:: ",product_name)
    query = {
        "query": {
            "function_score": {
                "functions": [
                    {
                        "field_value_factor": {
                            "field": "chains_count",
                            "factor": 1,
                            "missing": 0,
                        }
                    },
                    {
                        "field_value_factor": {
                            "field": "stores_count",
                            "factor": 0.1,
                            "missing": 0,
                        }
                    }
                ],
                "query": {
                    "bool": {
                        "must": {"match": {"name": product_name}},
                        "should": {
                            "span_first": {
                                "match": {
                                    "span_term": {"name": product_name}
                                },
                                "end": 1
                            }
                        }
                    }
                },
                "score_mode": "sum"
            }
        }
    }
    rel = es.search(index='products', body=query)
    max_score = 0
    max_result = {}
    temp = []
    for hit in rel['hits']['hits']:
        if max_score < hit['_score']:
            max_score = hit['_score']
            max_result = hit['_source']
        temp.append(hit['_source'])

    # Create a dataframe.
    df_result = pd.DataFrame([max_result])
    if not df_result.empty:
        df_result['product_quantity'] = product_quantity
    print(df_result)
    return df_result

def get_store_data_from_elastic(store_id, chain_name):
    query = {
        "query":{
            "bool":{
                "must":[
                    {
                        "match": {"store_id": store_id}
                    },
                    {
                        "match": {"ChainName": chain_name}
                    }
                ]
            }
        }
    }
    rel = es.search(index='stores_index', body=query)
    try:
        store_name = rel['hits']['hits'][0]['_source']['StoreName']
        store_lat = rel['hits']['hits'][0]['_source']['Latitude']
        store_lon = rel['hits']['hits'][0]['_source']['Longitude']
    except:
        store_name = "store name not found"
    return store_name, store_lat, store_lon


def covert_aff_to_store_name (min_aff):
    # 083@victory
    store_id = min_aff.split('@',1)[0]
    he_chain_name =c.retailer_dict[min_aff.split('@',1)[1]]
    store_data = get_store_data_from_elastic(store_id, he_chain_name)
    he_store_name = store_data[0]
    store_lat = store_data[1]
    store_lon = store_data[2]
    print("the store name is ",he_store_name)
    return he_chain_name+' '+he_store_name, store_lat, store_lon


def calc_closest_stores (shopping_list_sum, location):
    stores = pd.DataFrame(shopping_list_sum.index.map(covert_aff_to_store_name))
    stores['price'] = shopping_list_sum.values
    print(stores)
    stores[['name','lat', 'lon']] = pd.DataFrame(stores[stores.columns[0]].tolist())
    stores['distance'] = stores.apply(lambda row: get_distance(location.latitude, location.longitude, row['lat'], row['lon']),axis = 1)
    three_closest_stores = stores.nsmallest(3, 'distance')
    print(three_closest_stores, type(three_closest_stores))
    return three_closest_stores


# Start
def start(message):
    request = message.text
    if request == "/start" or request == "start" or request=='היי':
        return True
    else:
        return False


@bot.message_handler(func=start)
def start_p(message):
    resp_choice = 'הקלד "רשימת קניות" לחישוב הסל הזול ביותר או הקלד top5 לגלות את 5 המוצרים המבוקשים ביותר'
    choice = bot.send_message(message.chat.id, resp_choice)
    bot.register_next_step_handler(choice, choices_handler)


def choices_handler(message):
    choice = message.text
    print("the choice is: ", choice)
    if choice.lower() == "top5":
        resp_calc = 'מחשב...'
        bot.send_message(message.chat.id, resp_calc)
        top5_handler(message)
        # return True
    else:
        resp_address = 'אנא הכנס את הכתובת/המיקום שלך'
        sent_msg = bot.send_message(message.chat.id, resp_address)
        bot.register_next_step_handler(sent_msg, address_handler)
        # return False


# @bot.message_handler(func=choices_handler)
def top5_handler(message):
    print('calculation top 5')
    top5 = calc_top_5()
    top5_print = top5['name'].to_string(index=False)
    # top5['name'].values.tolist().
    bot.send_message(message.chat.id, top5_print)


#handle the address
def address_handler(message):
     address = message.text
     location = geocode_p(address)
     resp = c.text
     groceries_msg = bot.send_message(message.chat.id, resp)
     bot.register_next_step_handler(groceries_msg, send_cheapest_price, location)


# SEND TO KAFKA
def send_to_kafka(df):
    topics = "shopping_bot"
    producer = KafkaProducer(bootstrap_servers=c.bootstrapServers)
    # Getting the data ready for kafka
    row = df.to_dict(orient='records')[0]
    row_json_str = json.dumps(row)
    print(row)
    producer.send(topics, value=row_json_str.encode('utf-8'))
    producer.flush()

# @bot.message_handler(func=groceries_request)
def send_cheapest_price(message, location):
    if message.text == 'היי':
        print("the message is:",message.text)
        resp_again = bot.send_message(message.chat.id, 'רוצה שוב?')
        return bot.register_next_step_handler(resp_again, start)
    resp = 'קיבלתי את המידע,מעבד ומחזיר תשובה'
    print(location)
    bot.send_message(message.chat.id, resp)
    product_list = []
    shopping_cart=pd.DataFrame()
    for ingredient in message.text.split(','):
        product_name = ' '.join(ingredient.split()[0:-1])
        product_quantity = ingredient.split()[-1]
        product_list.append({"product_name": product_name, "product_quantity": product_quantity})
        if product_list[0]['product_name'] == '' or product_list[0]['product_quantity'] == '':
            print("something is wrong")
            resp = 'קלט שגוי'
            bot.send_message(message.chat.id, resp)
            return
        elastic_df = get_product_date_from_elastic(product_name, product_quantity)
        if elastic_df.empty:
            resp_bad_product = f"לא נמצא כלל המוצר שביקשת - נסה שוב- המוצר הוא {product_name}"
            resp = bot.send_message(message.chat.id, resp_bad_product)
            return bot.register_next_step_handler(resp, send_cheapest_price, location)
        if shopping_cart.empty:
            shopping_cart = elastic_df
        else:
            shopping_cart = pd.concat([shopping_cart, elastic_df])
    print(shopping_cart)
    print("###############################################################################")
    shopping_cart = pd.concat([shopping_cart.drop(['chains'], axis=1), shopping_cart['chains'].apply(pd.Series)], axis=1)
    print(shopping_cart)
    for retailer in c.retailer_dict.keys():
        if retailer in shopping_cart:
            shopping_cart[retailer] = shopping_cart[retailer].apply(add_retailer_names, retailer_name = retailer)
            shopping_cart = pd.concat([shopping_cart.drop([retailer], axis=1), shopping_cart[retailer].apply(pd.Series)], axis=1)
    print(shopping_cart)
    final_shopping_list = shopping_cart[shopping_cart.columns[~shopping_cart.isnull().any()]]
    print(final_shopping_list)
    user_id = message.from_user.id
    user_user_name = message.from_user.username
    user_first_name = message.from_user.first_name
    user_last_name = message.from_user.last_name
    chat_id = message.chat.id
    final_shopping_list[final_shopping_list.columns[4:]] = final_shopping_list[final_shopping_list.columns[4:]].astype('float64')
    prices_cols = final_shopping_list.columns[5:]
    try:
        shopping_list_sum = final_shopping_list[prices_cols].multiply(final_shopping_list['product_quantity'], axis="index").sum()
        print("the shopping list sum is: ",shopping_list_sum, type(shopping_list_sum))
    except Exception as e:
        print(e)
        resp = 'קלט שגוי'
        bot.send_message(message.chat.id, resp)
        return
    if shopping_list_sum.empty:
        resp_bad_list = f"לא נמצא סניף המכיל את כלל המוצרים המובקשים נסה ללא אחד המוצרים"
        resp = bot.send_message(message.chat.id, resp_bad_list)
        return bot.register_next_step_handler(resp, send_cheapest_price, location)
    #function that gets the sopping list sum and return only 5 closest and then on this do idxmin
    three_closest_stores = calc_closest_stores(shopping_list_sum, location)
    print('??????????????')
    # print(three_closest_stores)
    min_aff = three_closest_stores.nsmallest(1,'price')
    # aff = covert_aff_to_store_name(min_aff)
    min_aff_name = min_aff['name'].values[0]
    try:
        dist_from_store = get_distance(min_aff['lat'].values[0],min_aff['lon'].values[0],location.latitude, location.longitude)
        resp_dist = f"המרחק מהסניף הוא {dist_from_store} מטרים "
    except:
        resp_dist = f"מרחק לא חושב כי כתובת מגורים לא תקינה"
    min_price = round(shopping_list_sum.min(),2)
    resp_min_aff = f" הסניף הזול ביותר הוא {min_aff_name}"
    resp_min_price = f"המחיר הזול ביותר הוא {min_price}"
    bot.send_message(message.chat.id, resp_min_aff)
    bot.send_message(message.chat.id, resp_min_price)
    bot.send_message(message.chat.id, resp_dist)
    # Dataframe Enrichment
    products_from_elastic = final_shopping_list[['name','product_quantity']].to_dict('records')
    df_enrich_dict = {'product_list': products_from_elastic, 'chat_id': chat_id, 'user_id': user_id,
                      'user_user_name': user_user_name, 'user_first_name': user_first_name,
                      'user_last_name': user_last_name, 'min_aff': min_aff_name, 'min_price': min_price
                      }
    # product_to_list(products_from_elastic)
    groceries_answer = bot.send_message(message.chat.id,product_to_list(products_from_elastic))
    # use this row to add a df with the data from cheapest shopping cart
    df_enr = enrich_df(pd.DataFrame(), df_enrich_dict)
    print(df_enr)
    send_to_kafka(df_enr)
    return bot.register_next_step_handler(groceries_answer, send_cheapest_price, location)






bot.polling()