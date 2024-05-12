import requests
from bs4 import BeautifulSoup as bs
import uuid
import csv
import re

def extract():
    sources = ['https://www.dawn.com/', 'https://www.bbc.com/']
    articles_data = []

    for source in sources:
        print(f"Extracting data from: {source}")
        reqs = requests.get(source)
        soup = bs(reqs.text, 'html.parser')

        # Extract titles and descriptions
        for article in soup.find_all('article'):
            title = article.find('a').text.strip() if article.find('a') else "No title"
            description = article.find('p').text.strip() if article.find('p') else "No description"
            link = article.find('a', href=True)
            if link:
                link = link['href']
            else:
                link = "No link"
            articles_data.append({'title': title, 'description': description, 'source': source, 'link': link})

    return articles_data

def clean_text(text):
    text = re.sub('<[^<]+?>', '', text)  # Remove HTML tags
    text = re.sub('\s+', ' ', text).strip()  # Replace multiple spaces with a single space
    return text

def save_data(articles, filename='articles_detailed.csv'):
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['id', 'title', 'description', 'source', 'link'])
        writer.writeheader()
        for article in articles:
            writer.writerow({
                'id': str(uuid.uuid4()),
                'title': clean_text(article['title']),
                'description': clean_text(article['description']),
                'source': article['source'],
                'link': article['link']
            })

def main():
    articles = extract()
    save_data(articles)

if __name__ == "__main__":
    main()
