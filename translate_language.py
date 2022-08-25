"""
Translate tweets in BigQuery to English
"""

import os
import time

from google.cloud import bigquery
from google.cloud import translate
from google.cloud.exceptions import NotFound

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'
PROJECT_ID = 'YOUR_PROJECT_ID'
DATASET = 'coffee'


# Function to translate text from source_language to English
def translate_text(text, source_language):
    tr_client = translate.TranslationServiceClient()
    location = 'global'
    parent = f'projects/{PROJECT_ID}/locations/{location}'

    response = tr_client.translate_text(
        request={
            'parent': parent,
            'contents': [text],
            'mime_type': 'text/plain',  # mime types: text/plain, text/html
            'source_language_code': source_language,
            'target_language_code': 'en',
        }
    )
    return response.translations[0].translated_text


# Helper function to write data into BigQuery
def insert_bq(client, table, data):
    attempts = 10
    while attempts > 0:
        try:
            client.insert_rows(table, data)
        except Exception as e:
            # print(e)
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

    # Create new table for translations
    new_table_id = f'{PROJECT_ID}.{DATASET}.translations'
    new_schema = [
        bigquery.SchemaField('id', "STRING", mode='REQUIRED'),
        bigquery.SchemaField('text', "STRING", mode='REQUIRED'),
        bigquery.SchemaField('translation', "STRING", mode='REQUIRED'),
    ]
    new_table = bigquery.Table(new_table_id, schema=new_schema)
    try:
        new_table = bq_client.get_table(new_table_id)
        print(f'Table {table_id} already exists')
    except NotFound:
        new_table = bq_client.create_table(new_table)
        print(f'New table {new_table_id} created')
        time.sleep(10)   # wait a few seconds before new table is ready to be written to

    # Filter on tweets written in languages other than English that also support sentiment analysis
    query = f"""
                SELECT * FROM `{PROJECT_ID}.{DATASET}.tweets` AS t 
                WHERE ARRAY_LENGTH(t.referenced_tweets)=0 AND 
                lang IN ("ar", "zh", "nl", "fr", "de", "id", "it", "ja", "ko", "pt", "es", "th", "tr", "vi") 
                ORDER BY public_metrics.like_count DESC;
            """
    query_job = bq_client.query(query)
    results = query_job.result()
    total = results.total_rows
    df = query_job.to_dataframe()
    df = df.reset_index()   # make sure indexes pair with number of rows

    # Insert translations into new table
    for index, row in df.iterrows():
        try:
            translation = translate_text(row['text'], row['lang'])
            print(f'{index+1}/{total}: {translation}\n')

            new_row = {
                'id': row['id'],
                'text': row['text'],
                'translation': translation
            }
            insert_bq(bq_client, new_table, [new_row])
        except Exception as e:
            print(e)


if __name__ == '__main__':
    main()
