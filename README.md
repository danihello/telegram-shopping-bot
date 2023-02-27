# telegram-shopping-bot

This is the final project for the "Data Engineering" course by Naya College.

In this project we create an ETL pipeline to scrape data regarding prices from several Israeli retailers: shufersal, rami-levi, victory.

we clean this data and store it for several porpuses:

* Analysis -Store the data and allow users to conduct historical pricing analysis
* Trens - Create a dashboard and Show price trends over time
* Smart consumerism - Create a telegram bot that will assist in finding the best prices
* Geo-friendly - Find the best prices in a location convenient  to the users

The project architecture:

![image](https://user-images.githubusercontent.com/42510219/221525395-5b867a0f-96c0-46a7-afa6-c4ccacc3eeb1.png)


Tools for this project include many big data tools such as:

* Airflow
* Elasticsearch 
* Kafka
* Spark
* mongodb
* S3
* Amazon athena

Dashboard was created using power bi

we created a telegram bot using python packages

data scraping involved beautifulsoup and selenium
