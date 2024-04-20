
# AWS-Data-Pipeline-Project-Implementing-Data-Validation-Using-Lambda-based-Gluecrawler-v1.0

## Business Requirement:
A renowned Automobile Car Firm has several franchises across a region and they want to govern their daily automobile products orders created from their retail outlets.

So throughout the business operations, they receive the sales orders transaction files through sftp in a batch of 100 records csv file in an interval of ~2-3 hours. Each time, the financial analyst needs to open a file and cross validate the correct records and separate out the corrupt records to make the analysis batch file ready for analysis. Moreover, they need to run scripts to load data to SQL Engine manually to run simple aggregation queries for generating reports. Many times this intermediate process of acquiring notification of delivered batch file,cleaning and loading to SQL gets delayed due to manual interventions.

Therefore, the firm needs to improve their business process by automating the tasks, generating the valid files and making data available for running queries at full availability.

## Solution

To solve the problem stated in the above case, AWS Cloud Platform is introduced to migrate traditional on premise operations to modern data analytics stack.

- S3 Bucket is a popular storage resource as a data lake for our incoming batch files.
- Also the new batch data could arrive in ~2 hours so need a tool which gets triggered on the instant the file gets delivered. AWS Lambda is a required entity in such a case.
- To automate a loading of cleaned data in SQL and for analytics purposes. The best solution is AWS Glue Crawler which is a serverless tool and runs on a data source defined to crawl data and automatically creates a relational table with schema detection. For running queries over the table we can use AWS Athena which runs on presto SQL to run analytics queries over a crawled table.
- Again AWS Crawler could be run manually or at a scheduled time interval by cron job support. But since start time is not predictable, it is better to trigger a crawler using another lambda function to start crawler on arrival to validate clean batch file successively.

## AWS Services Required


| AWS Services | Alias_Name     | Description                |
| :-------- | :------- | :------------------------- |
| `S3 Object Storage` | `autosalesraw-batch-2024` | DataLake for arrival of Batch incoming file. |
| `S3 Object Storage`      | `auto-salesfiltered-batch2024` | Staging Area, stores valid records & corrupt records in separate subfolders. |
| `IAM Role`      | `data_validator_lambda_role` | Lambda role for data validation python script run. |
| `IAM Role`      | `glue-crawler-lambda-role` | Lambda role to trigger glue crawler|
| `IAM Role`      | `gluerunner` | Lambda role to trigger Glue crawler|
| `Lambda Function`      | `data-validation-runner` | Lambda function to trigger python Validation script |
| `Lambda Function`      | `crawler-runner-func` | Lambda function to trigger Crawler |
| `Glue Crawler`      | `AutoSalesCrawler` | Crawler to run over valid batch files and generate table. |



## Setup of Services

### S3 Buckets:
s3 setup: im

### IAM Roles:
Role alias name: data_validator_lambda_role

Permissions: img in doc

Role alias name: glue-crawler-lambda-role

Permissions: img in doc

Role alias name: gluerunner

Permissions: img in doc

### Lambda Fucntions:
img in folder [lambda_list]

#### 1. data-validation-runner
img in folder 
- Triggers on: `s3://autosalesraw-batch-2024/`
- Events Type: `COPY`,`PUT`,`POST`

#### 2. crawler-runner-func

- Triggers on: `s3://auto-salesfiltered-batch2024/valid_batch_files/`
- Events Type: `COPY`,`PUT`,`POST`

### Glue Crawler:

#### AutoSalesCrawler

- Database: `autosales_DB`
- Target: `s3://auto-salesfiltered-batch2024/valid_batch_files/`

## Workflow

- Input batch file lands first in S3 bucket `autosalesraw-batch-2024`, which triggers the lambda function `data-validation-runner` because of events defined (PUT,COPY,POST).

- On successful run, valid records saved into `s3://auto-salesfiltered-batch2024/valid_batch_files/` and if corrupt records found then saved to `s3://auto-salesfiltered-batch2024/corrupt_batch_files/`.

- Now other lambda function will immediately trigger on valid file generation in `valid_batch_files/` and start crawler job.

- On successful run of `crawler-runner-func`, a table would be created in autosales_DB.


## Unit Testing

Unit testing is performed manually to verify the pipeline ability to perform intended task.

- In one go, First lambda function is tested by upload of sample file `auto_sales_batch-123.csv` in autosalesraw-batch-2024 bucket. Triggered the lambda instantly and output files observed in valid & corrupt batch folders in other auto-salesfiltered-batch2024 bucket.


- Again, to check other lambda function for crawler, a file uploaded to valid_batch_files folder with all valid records. 
- It is observed lambda successfully started glue crawler and created a table at end of job.

## Integration Testing

- Now for testing whole pipeline operation matches our expectation, integration testing also performed in one go. Another batch file uploaded to `auto_sales_batch-123.csv` as `auto_sales_batch-234.csv`. 
- First lambda started and performed successful generation of valid & corrupt records.

- At same instant, as valid_batch_files folder updated. Other lambda function got triggered to run crawler.

- After completion, incremental data is loaded to our table which was generated in first unit test. To verify, SQL query was ran in AWS Athena. With this pipeline successfully matched our expectation in timely response and operations. 


