import sys
sys.path.insert(0, '../lambda_scripts')
from utils import validator,output_file_formatter,s3_writer
import pytest
from moto import mock_aws
import boto3


@pytest.fixture
def s3_boto():
    s3 = boto3.client('s3',region_name='us-east-1',aws_access_key_id="AWS_ACCESS_KEY_ID",aws_secret_access_key="AWS_SECRET_ACCESS_KEY")
    return s3

@pytest.fixture
def params():
    para={}
    para['input_file'] = "/lambda_scripts/hello/_.csv"
    para['content'] ='''transaction_id,automobile_name,order_date,booked_price\n2345,,,22333'''
    return para


def test_output_alias(params):
    input_file = params['input_file']
    valid_key,corrupt_key = output_file_formatter(input_file)

    assert valid_key == "valid_batch_files/__validrecords.csv"
    assert corrupt_key == "corrupt_batch_files/__corruptrecords.csv","Invalid_alias"

def test_validator(params):
    content = params['content']
    valid,corrupt,corrupt_count = validator(content)
    
    assert "transaction_id,automobile_name,order_date,booked_price" in corrupt,"Header not appended"
    assert '2345,,,22333' in corrupt
    assert '2345' in corrupt
    assert corrupt_count == 1

@mock_aws
def test_s3(s3_boto):
    bucket = "auto-salesfiltered-batch2024"
    key1 = "validkey"
    key2 = "corruptkey"
    body = "Content of body"

    #create output s3 bucket as s3_writer requires.

    s3_boto.create_bucket(Bucket=bucket)
    s3_writer(s3_boto,validoutput_s3_key=key1,corruptoutput_s3_key=key2,corrupt_row=1 ,valid = body ,corrupt = body)
    response = s3_boto.get_object(Bucket=bucket, Key=key1)
    valid_body = response['Body'].read().decode('utf-8')
    result = s3_boto.list_buckets()
    assert "Content of body" == valid_body
    assert "auto-salesfiltered-batch2024" in (result["Buckets"][0]["Name"])
    
