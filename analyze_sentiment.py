"""
Analyze tweets stored in BigQuery for sentiment
"""

import os
import time

from google.cloud import bigquery
from google.cloud import language
from google.cloud.exceptions import NotFound

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'
PROJECT_ID = 'YOUR_PROJECT_ID'
DATASET = 'coffee'


# Function to analyze a piece of text in source_language, then return sentiment & magnitude scores
def analyze_sentiment(text, source_language):
    la_client = language.LanguageServiceClient()
    document = language.Document(content=text, type_=language.Document.Type.PLAIN_TEXT, language=source_language)

    sentiment = la_client.analyze_sentiment(request={'document': document}).document_sentiment
    return round(sentiment.score, 3), round(sentiment.magnitude, 3)


# Helper function to write data into BigQuery
def insert_bq(client, table, data):
    attempts = 10
    while attempts > 0:
        try:
            client.insert_rows(table, data)
        except Exception as e:
            print(e)
            time.sleep(0.2)
            attempts -= 1
        else:
            break


def main():
    # Instantiate BigQuery client
    bq_client = bigquery.Client()
    table_id = f'{PROJECT_ID}.{DATASET}.tweets'

    # Retrieve schema of existing table
    table = bq_client.get_table(table_id)
    original_schema = table.schema

    # Create new table for sentiment
    new_table_id = f'{PROJECT_ID}.{DATASET}.sentiments'
    new_schema = original_schema[:]  # create a copy of the schema
    new_schema.append(bigquery.SchemaField('score', 'FLOAT'))
    new_schema.append(bigquery.SchemaField('magnitude', 'FLOAT'))
    new_table = bigquery.Table(new_table_id, schema=new_schema)
    try:
        new_table = bq_client.get_table(new_table_id)
        print(f'Table {table_id} already exists')
    except NotFound:
        new_table = bq_client.create_table(new_table)
        print(f'New table {new_table_id} created')
        time.sleep(10)  # wait a few seconds before new table is ready to be written to

    # Make query
    query = f"""
                SELECT * FROM `{PROJECT_ID}.{DATASET}.tweets` AS t 
                WHERE ARRAY_LENGTH(t.referenced_tweets)=0 AND 
                lang IN ("ar", "zh", "nl", "fr", "de", "en", "id", "it", "ja", "ko", "pt", "es", "th", "tr", "vi") 
                ORDER BY public_metrics.like_count DESC;
            """
    query_job = bq_client.query(query)
    results = query_job.result()
    total = results.total_rows
    df = query_job.to_dataframe()
    df = df.reset_index()   # make sure indexes pair with number of rows

    # Insert sentiment scores into new table
    for index, row in df.iterrows():
        try:
            score, magnitude = analyze_sentiment(row['text'], row['lang'])
            print(f'{index + 1}/{total}: {score}, {magnitude}')

            row.pop('index')
            row['score'] = score
            row['magnitude'] = magnitude
            insert_bq(bq_client, new_table, [row])
        except Exception as e:
            print(e)


if __name__ == '__main__':
    main()
