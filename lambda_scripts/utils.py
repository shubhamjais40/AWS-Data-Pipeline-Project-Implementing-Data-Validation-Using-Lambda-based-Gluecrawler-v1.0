import csv
from io import StringIO
from pathlib import Path

def validator(s3_content: str):
        '''
        :param: s3_content as string response from s3 bucket,lambda funciton targeted on.
        returns: valid rows,corrupt rows and corrupt_row counts.

        '''
        valid = ''
        corrupt = ''
        corrupt_row = 0

        csv_reader = csv.reader(StringIO(s3_content))
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
        
        return valid,corrupt,corrupt_row


def output_file_formatter(input_s3_key:str,):
    '''
    output_file_formatter takes param input file path or input s3 bucket path,
    and returns alias name of output files to be written to output s3 bucket.
    '''
    output_alias = Path(input_s3_key).stem
    validoutput_key = f"valid_batch_files/{output_alias}_validrecords.csv"
    corruptoutput_key = f"corrupt_batch_files/{output_alias}_corruptrecords.csv"
    return validoutput_key,corruptoutput_key

def s3_writer(s3_client,validoutput_s3_key,corruptoutput_s3_key,corrupt_row,valid,corrupt):
    '''
    s3_writer: writes the validated records to output s3 bucket.

    :Params:
    s3_client: boto3 s3 resource client
    validoutput_s3_key: alias name of valid output file dervied from output formatter
    corruptoutput_s3_key: alias name of corrupt output file dervied from output formatter
    corrupt_row: count of invalid or corrupt rows found
    valid: content of valid rows to be written
    corrupt: content of corrupt rows to be written
    '''


    try:
        s3_client.put_object(Bucket="auto-salesfiltered-batch2024", Key=validoutput_s3_key, Body=valid)
        print("Valid file writtent successfully...")

        if corrupt_row > 0:
            print(f"Total corrupt rows found {corrupt_row}, hence writing to corrupt_batch_file...")
            s3_client.put_object(Bucket="auto-salesfiltered-batch2024", Key=corruptoutput_s3_key, Body=corrupt)
            print("Corrupt file written successfully...")
        

    except Exception as e:
        print(f"Error writing data to S3: {str(e)}")
        return False 