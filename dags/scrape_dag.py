from datetime import datetime, timedelta  # Import timedelta correctly here
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from bs4 import BeautifulSoup
import re
import csv
import uuid

# Define your data sources
sources = ['https://www.dawn.com/', 'https://www.bbc.com/']

def extract():
    articles_data = []
    for source in sources:
        print(f"Extracting data from: {source}")
        reqs = requests.get(source)
        soup = BeautifulSoup(reqs.text, 'html.parser')

        # Extract titles, descriptions, and links
        for article in soup.find_all('article'):
            title = article.find('a').text.strip() if article.find('a') else "No title"
            description = article.find('p').text.strip() if article.find('p') else "No description"
            link = article.find('a', href=True)['href'] if article.find('a', href=True) else "No link"
            articles_data.append({
                'title': title,
                'description': description,
                'source': source,
                'link': link
            })
    return articles_data

def transform(articles_data):
    preprocessed_data = []
    for article in articles_data:
        article['title'] = clean_text(article['title'])
        article['description'] = clean_text(article['description'])
        preprocessed_data.append(article)
    return preprocessed_data

def clean_text(text):
    text = re.sub('<[^<]+?>', '', text)  # Remove HTML tags
    text = re.sub('\s+', ' ', text).strip()  # Replace multiple spaces with a single space
    return text

def load(preprocessed_data):
    filename = 'articles_detailed.csv'
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['id', 'title', 'description', 'source', 'link'])
        writer.writeheader()
        for article in preprocessed_data:
            article['id'] = str(uuid.uuid4())
            writer.writerow(article)
    print(f"Data saved to {filename}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # Use timedelta correctly
}

with DAG(
    'web_scraping_mlops_dag',
    default_args=default_args,
    description='A DAG for scraping, processing, and saving web data',
    schedule_interval='@daily',  # Runs once every day
    catchup=False,  # Prevents backfilling of the DAG
) as dag:

    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
        op_args=[task_extract.output],
    )

    task_load = PythonOperator(
        task_id='load',
        python_callable=load,
        op_args=[task_transform.output],
    )

    task_extract >> task_transform >> task_load
