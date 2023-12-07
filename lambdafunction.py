# put this file in lambda script part and deploy it
import boto3
import csv
import io
import time

def lambda_handler(event, context):
    source_bucket_name = 'sourcedataforsnowflake'
    destination_bucket_name = 'destinationdataforsnowflake'
    source_key = event['Records'][0]['s3']['object']['key']

    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=source_bucket_name, Key=source_key)
    csv_body = response['Body'].read().decode('utf-8')
    csv_reader = csv.DictReader(io.StringIO(csv_body))
    rows = [row for row in csv_reader]

    batch_size = 100
    wait_time = 15
    s3_resource = boto3.resource('s3')

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]

        selected_columns = ['review_id', 'author_name', 'review_text', 'review_likes', 'review_timestamp']
        processed_batch = '\n'.join([','.join([row[column] for column in selected_columns]) for row in batch])

        s3_resource.Object(destination_bucket_name, f"processed_data_batch_{i}_{time.time()}.csv").put(Body=processed_batch.encode('utf-8'))

        time.sleep(wait_time)