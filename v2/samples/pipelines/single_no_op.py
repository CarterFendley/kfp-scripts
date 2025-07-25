from kfp import dsl

from v2.samples.components import no_op

@dsl.pipeline()
def single_no_op():
    no_op()


if __name__ == '__main__':
    from kfp.client import Client

    client = Client()
    client.create_run_from_pipeline_func(
        pipeline_func=single_no_op
    )