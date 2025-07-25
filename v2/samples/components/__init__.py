from kfp import dsl

@dsl.component()
def no_op():
    pass