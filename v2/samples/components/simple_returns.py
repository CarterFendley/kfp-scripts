from kfp import dsl

@dsl.component
def return_int(number: int) -> int:
    return number