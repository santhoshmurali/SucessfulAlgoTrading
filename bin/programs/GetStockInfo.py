# Importing necessary Libraries
import pandas as pd
import yfinance as yf
import requests
### These are the libraries we need for writing the data to s3
### https://khandelwal-shekhar.medium.com/read-and-write-to-from-s3-using-python-boto3-and-pandas-s3fs-144341e23aa1
import io   
from io import StringIO
import yaml
import boto3
import os


# config_file path
aws_api_key_path = r"..\\config\\cred_config.yaml"
config_file_path = r"..\\config\\config.yaml"

with open(aws_api_key_path,'r') as aws_api_file:
    aws_api_config = yaml.safe_load(aws_api_file)

with open(config_file_path,'r') as config_file:
    config = yaml.safe_load(config_file)


#Configuring the AWS keys
AWS_S3_BUCKET = aws_api_config.get('AWS_S3_BUCKET')
AWS_ACCESS_KEY_ID = aws_api_config.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY  = aws_api_config.get('AWS_SECRET_ACCESS_KEY')


#Common Configs
index_output_data_file_name = config.get('index_path_50')[1]
index_output_detailed_file_name = config.get('index_path_50')[2]
index_path = config.get('index_path_50')[0]
excluded_keys_info = config.get('excluded_keys_info') #['companyOfficers']



# Setting up s3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name = 'us-east-1'
)

# Function to convert the data read from live csv to a buffer then to pandas dataframe without storing it
#### -Not used in the Program
#def csvbuffer_to_dataframe(contents):
#    content_str = contents.decode('utf-8')
#    content_df = pd.read_csv(StringIO(content_str))
#    return(content_df)




headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
}


# Downlaod the latest data and writing to s3
def process_index_master():
    response = requests.get(index_path, headers=headers)
    contents = io.StringIO(response.content.decode('utf-8')).getvalue()
    response_s3 = s3_client.put_object(
        Bucket = AWS_S3_BUCKET,
        Key = index_output_data_file_name,
        Body = contents
    )

    ## Get detailed and expanded data for those stocks
    n50 = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
    def without_keys(d,keys):
        return {x:d[x] for x in d if x not in keys}
    pd.DataFrame([without_keys(yf.Ticker(x+'.NS').info,excluded_keys_info) for x in n50['Symbol']]).to_csv(
        f"s3://{AWS_S3_BUCKET}/{index_output_detailed_file_name}",
        index=False,
        storage_options={
            "key": AWS_ACCESS_KEY_ID,
            "secret" : AWS_SECRET_ACCESS_KEY,
        }
    )
    return("Successfull!")

if __name__ == '__main__':
    process_index_master()
