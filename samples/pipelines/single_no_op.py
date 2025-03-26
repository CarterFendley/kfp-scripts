from kfp import dsl
from kfp.components import create_component_from_func

from samples.components import no_op

@dsl.pipeline(
    name='Single no op',
)
def single_no_op():
    c = create_component_from_func(func=no_op)
    c()