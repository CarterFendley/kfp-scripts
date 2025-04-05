from kfp import dsl
from kfp.components import create_component_from_func

from samples.components import runtime_exception

def transformer_disable_caching(op):
    op.execution_options.caching_strategy.max_cache_staleness = "P0D"

def errors():
    # Disable caching for simplicity of simulation
    dsl.get_pipeline_conf().add_op_transformer(transformer_disable_caching)

    c = create_component_from_func(func=runtime_exception)
    c()
