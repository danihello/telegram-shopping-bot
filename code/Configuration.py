# ========================================================================================================= #
# ========================================Telegram Config ================================================= #
# ========================================================================================================= #
TOKEN = '5698262338:AAGtso-3cB2fcYjIor8pStHzLp3DG69s4KA'
text = '''שלום אני בוט למציאת סל הקניות הזול ביותר באיזורך
אני אעזור לך במציאת סל הקניות הזול ביותר בצורה פשוטה ומהירה
אני יכול לחפש מצרכים באופן הבא:
הקלד את שם המוצר הרצוי ולאחריו את הכמות הרצויה והפרד בין המוצרים בעזרת פסיק
נסה לדייק ככל הניתן את שם המוצר
לדוגמה:
עגבניה 1, חלב 2, בשר טחון 2, תבנית 12 ביצים 1,יוגורט דנונה 1, תפוח פינק 2, תירס מתוק קופסה 1'''
retailer_dict={"shufersal":'שופרסל', "victory":'ויקטורי', "ramilevi":'רמי לוי'}
# ========================================================================================================= #
# ========================================Kafka Connections =============================================== #
# ========================================================================================================= #
bootstrapServers = "cnt7-naya-cdh63:9092"
topics = "shopping_bot"
# topic_kafka_to_mongo = 'From_Kafka_To_Mongo'

# ========================================================================================================= #
# ========================================Mongo =========================================================== #
# ========================================================================================================= #
mongo_host = 'mongodb://localhost:27017/'
db_name = 'shopping_carts_db'
col_name = 'Shopping_Carts_Requests'

#=============geocode=API===============#
geocode_api_key = 'aTCHXMX6776rQA4cApA6zMG9Ywlqp2419UN4oG0fniU'

distance_api_key = '5b3ce3597851110001cf62484f7fb259d0624e12ba68376aebd9bf37'

#===================KAFKA==============#

brokers = ['cnt7-naya-cdh63:9092']
stores_topic = 'kafka-stores'
products_topic = 'Kafka-products_fp'
bootstrapservers = "cnt7-naya-cdh63:9092"

#==================stores=============#

shufersal_download_path = '/home/naya/finalproject/downloads/shufersal/'
shufersal_stores = ['001','035','106','109','129','169','216','224','269','312','361','362','374','477','780']

victory_download_path = '/home/naya/finalproject/downloads/victory/'
victory_stores = ['002','008','014','022','027','041','046','051','059','061','068','073','074','081','083']

#==============elasticsearch============#

stores_idx = 'stores_index'

products_idx = 'products'

#==============AmazonAWS=S3==============#

aws_access_key='AKIATNRCFRHPUWJ5SV7O'
aws_secret_key = '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'
aws_region = 'us-east-1'
aws_bucket = 'de-fp-stores'
aws_stores_path ='stores/json/'
aws_products_path = 'products/json/'
