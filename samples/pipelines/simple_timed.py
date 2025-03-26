from kfp import dsl
from kfp.components import create_component_from_func

from samples.components import timed_sleep

def transformer_disable_caching(op):
    op.execution_options.caching_strategy.max_cache_staleness = "P0D"

def simple_timed(base_time: int):
    # Set some parallelism to mess around with potential addition of projections with different parallelism
    dsl.get_pipeline_conf().set_parallelism(3)

    # Disable caching for simplicity of simulation
    dsl.get_pipeline_conf().add_op_transformer(transformer_disable_caching)

    c = create_component_from_func(func=timed_sleep)

    # Simple sequential components
    s1 = c(seconds=base_time)
    s1.set_display_name("Sequential 1")

    # Parallel for components
    with dsl.ParallelFor([1, 2, 3]) as additional:
        p1 = c(seconds=base_time, additional=additional)
        p1.after(s1) # Run for loop after sequential components
        p1.set_display_name(f"Parallel")
    

    s2 = c(seconds=base_time)
    s2.set_display_name("Sequential 2")
