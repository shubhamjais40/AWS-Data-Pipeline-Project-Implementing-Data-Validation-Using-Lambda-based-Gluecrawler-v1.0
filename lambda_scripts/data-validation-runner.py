#import inbuilt python modules

import json
import os
import csv
from pathlib import Path
from io import StringIO
import boto3

def lambda_handler(event, context):
    s3_bucket = None
    input_s3_key = None
    
    for record in event["Records"]:
        s3_bucket = record["s3"]["bucket"]["name"]
        input_s3_key = record["s3"]["object"]["key"]

    # Initialize the S3 client
    s3 = boto3.client('s3')
    
    print("for output alias taking",input_s3_key)
    output_alias = Path(input_s3_key).stem  
    try:
        # Read the CSV file from S3
        print('Reading file ' + input_s3_key +  ' from S3 bucket : ' + s3_bucket)
        response = s3.get_object(Bucket=s3_bucket, Key=input_s3_key)
        csv_content = response['Body'].read().decode('utf-8')

        valid = ''
        corrupt = ''
        corrupt_row = 0
        
        csv_reader = csv.reader(StringIO(csv_content))
        header = next(csv_reader)
        header = ','.join(header) + '\n'
        valid +=header
        corrupt +=header
        for row in csv_reader:
            if ((len(row)==4) and (len([True for item in row if item != ''])==4)):
                valid += ",".join(row) + "\n"
            else:
                print(f"Invalid records found:{row}")
                corrupt_row += 1
                corrupt += ",".join(row) + "\n"
                
        #output file alias name formating        
        validoutput_s3_key = f"valid_batch_files/{output_alias}_validrecords.csv"
        corruptoutput_s3_key = f'corrupt_batch_files/{output_alias}_corruptrecords.csv'
        

        s3.put_object(Bucket="auto-salesfiltered-batch2024", Key=validoutput_s3_key, Body=valid)
        
        
        # if corrupt row count is > 0 then only write file to corrupt_batch_file
        if corrupt_row > 0:
            print(f"Total corrupt rows found {corrupt_row}, hence writing to corrupt_batch_file...")
            s3.put_object(Bucket="auto-salesfiltered-batch2024", Key=corruptoutput_s3_key, Body=corrupt)
            
    except Exception as e:
        print(f"Error writing data to S3: {str(e)}")
        return False 