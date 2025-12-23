import os
import pandas as pd

STORAGE_OPTIONS = {
    'endpoint_url': 'http://localhost:8333',
    'key': os.environ['S3_ACCESS_KEY'],
    'secret': os.environ['S3_SECRET_KEY'],
}

def pull_dummy():
    df = pd.read_parquet(
        "s3://my-bucket/dummy_script.parquet",
        storage_options=STORAGE_OPTIONS
    )
    print(df)

if __name__ == '__main__':
    pull_dummy()