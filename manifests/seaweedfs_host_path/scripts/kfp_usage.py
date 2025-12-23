from kfp import dsl
from kfp import kubernetes


import os


@dsl.component(packages_to_install=["pandas", "pyarrow", "fsspec", "s3fs"])
def push():
    import os
    import pandas as pd

    STORAGE_OPTIONS = {
        'endpoint_url': 'http://seaweedfs.kubeflow.svc.cluster.local:8333',
        'key': os.environ["S3_ACCESS_KEY"],
        'secret': os.environ["S3_SECRET_KEY"],
    }

    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 8]})
    df.to_parquet(
        's3://my-bucket/dummy_kfp.parquet',
        storage_options=STORAGE_OPTIONS
    )

@dsl.component(packages_to_install=["pandas", "pyarrow", "fsspec", "s3fs"])
def pull():
    import os
    import pandas as pd

    STORAGE_OPTIONS = {
        'endpoint_url': 'http://seaweedfs.kubeflow.svc.cluster.local:8333',
        'key': os.environ["S3_ACCESS_KEY"],
        'secret': os.environ["S3_SECRET_KEY"],
    }

    df = pd.read_parquet(
        "s3://my-bucket/dummy_kfp.parquet",
        storage_options=STORAGE_OPTIONS
    )
    print(df)

@dsl.pipeline
def test_bucket():
    task_push = push()
    _ = kubernetes.use_secret_as_env(
        task_push,
        secret_name="mlpipeline-minio-artifact",
        secret_key_to_env={
            "accesskey": "S3_ACCESS_KEY",
            "secretkey": "S3_SECRET_KEY",
        }
    )

    task_pull = pull()
    _ = kubernetes.use_secret_as_env(
        task_pull,
        secret_name="mlpipeline-minio-artifact",
        secret_key_to_env={
            "accesskey": "S3_ACCESS_KEY",
            "secretkey": "S3_SECRET_KEY",
        }
    )

    task_pull.after(task_push)


if __name__ == '__main__':
    from kfp.client import Client

    client = Client()
    client.create_run_from_pipeline_func(
        pipeline_func=test_bucket,
        enable_caching=False
    )