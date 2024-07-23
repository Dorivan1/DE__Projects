import awswrangler as wr
import pandas as pd
import urllib.parse
import os

s3_cleansed_layer = os.environ['s3_cleansed_layer']
glue_catalog_db_name = os.environ['glue_catalog_db_name']
glue_catalog_table_name = os.environ['glue_catalog_table_name']
write_data_operation = os.environ['write_data_operation']

def lambda_handler(event, context):
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
       
        df_raw = wr.s3.read_json(f's3://{bucket}/{key}')

        
        df_normalized = pd.json_normalize(df_raw['items'])

        
        wr_response = wr.s3.to_parquet(
            df=df_normalized,
            path=s3_cleansed_layer,
            dataset=True,
            database=glue_catalog_db_name,
            table=glue_catalog_table_name,
            mode=write_data_operation
        )

        return wr_response
    except Exception as e:
        print(e)
        print(f'Error getting object {key} from bucket {bucket}. Make sure they exist and your bucket is in the same region as this function.')
        raise e