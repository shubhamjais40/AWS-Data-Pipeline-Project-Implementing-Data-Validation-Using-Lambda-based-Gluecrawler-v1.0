# lambda code for triggering glue crawler targeted over valid_records_batch


import boto3

def lambda_handler(event, context):
    glue_client = boto3.client('glue')

    '''Replace your crawler name in below parameter'''
    crawler_name = 'AutoSalesCrawler'

    response = glue_client.start_crawler(Name=crawler_name)

    return {
        'statusCode': 200,
        'body': 'Crawler started successfully: {}'.format(crawler_name)
    }
