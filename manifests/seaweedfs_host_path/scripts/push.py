import os
import pandas as pd

STORAGE_OPTIONS = {
    'endpoint_url': 'http://localhost:8333',
    'key': os.environ['S3_ACCESS_KEY'],
    'secret': os.environ['S3_SECRET_KEY'],
}

def push_dummy():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 8]})
    df.to_parquet(
        's3://my-bucket/dummy_script.parquet',
        storage_options=STORAGE_OPTIONS
    )

if __name__ == '__main__':
    push_dummy()