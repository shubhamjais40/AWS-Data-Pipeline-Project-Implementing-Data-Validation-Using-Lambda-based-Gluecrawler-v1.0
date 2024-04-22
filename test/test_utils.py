import sys
sys.path.insert(0, '../lambda_scripts')
from utils import s3_writer,validator,output_file_formatter
import pytest


input_file="/lambda_scripts/hello/_.csv"
content='''transaction_id,automobile_name,order_date,booked_price
2345,,,22333
'''

def test_output_alias():
    valid_key,corrupt_key = output_file_formatter(input_file)

    assert valid_key == "valid_batch_files/__validrecords.csv"
    assert corrupt_key == "corrupt_batch_files/__corruptrecords.csv","Invalid_alias"

def test_validator():
    v,c,cr = validator(content)
    #assert '2345,,,22333' in v
    assert "transaction_id,automobile_name,order_date,booked_price" in c,"Header not appended"
    assert '2345,,,22333' in c
    assert '2345' in c
    assert cr == 1
