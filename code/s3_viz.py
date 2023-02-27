import boto3
import io
import pandas as pd
import numpy as np
import json
from pytz import timezone
from datetime import datetime

def explode(df):
    df['tmp']=df.apply(lambda row: list(zip(row['chains'])), axis=1)
    df=df.explode('tmp')
    df[['chains']]=pd.DataFrame(df['tmp'].tolist(), index=df.index)
    df.drop(columns='tmp', inplace=True)
    return df

def parsing_products_daily_file():

    obj = s3_client.get_object(Bucket=bucket, Key=product_key)
    df = pd.read_json(io.BytesIO(obj['Body'].read()))
    data = explode(df)
    stores_data = df['chains'].apply(pd.Series)
    merged_df = pd.merge(data, stores_data, left_index=True, right_index=True)

    merged_df['stores_prices'] = np.where(merged_df['chains'] == 'victory', merged_df['victory'],
                                    np.where(merged_df['chains'] == 'shufersal', merged_df['shufersal'], merged_df['ramilevi']))
    merged_df = merged_df.drop(['victory', 'shufersal', 'ramilevi'], axis=1)

    merged_df['flatten_stores_prices'] = merged_df['stores_prices'].apply(lambda x: list(x.items()))
    merged_df = merged_df.drop(['stores_prices'], axis=1)

    final_df = merged_df.explode('flatten_stores_prices')

    final_df['store_id'] = final_df['flatten_stores_prices'].apply(lambda x: x[0])
    final_df['price'] = final_df['flatten_stores_prices'].apply(lambda x: x[1])
    final_df = final_df.drop(['flatten_stores_prices'], axis=1)
    final_df.to_parquet(f's3://de-fp-stores/products/parsed_jsons/{today}.parquet', index=False)

def parsing_stores_daily_file():
    obj = s3_client.get_object(Bucket=bucket, Key=store_key)
    df = pd.read_json(io.BytesIO(obj['Body'].read()))
    df.to_parquet(f's3://de-fp-stores/stores/parsed_jsons/{today}.parquet', index=False)

s3_client = boto3.client('s3')
bucket = 'de-fp-stores'
il_tz = timezone('Asia/Jerusalem')
today = datetime.now(il_tz).strftime("%Y-%m-%d")
product_key = f'products/json/{today}.json'
store_key = f'stores/json/{today}.json'
parsing_products_daily_file()
parsing_stores_daily_file()
print ('Daily files have been parsed and sent to the bucket.')