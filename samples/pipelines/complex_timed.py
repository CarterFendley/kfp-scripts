from kfp import dsl
from kfp.components import create_component_from_func

from samples.components import timed_sleep

def transformer_disable_caching(op):
    op.execution_options.caching_strategy.max_cache_staleness = "P0D"

def complex_timed(base_time: int):
    # Set some parallelism to mess around with potential addition of projections with different parallelism
    dsl.get_pipeline_conf().set_parallelism(3)

    # Disable caching for simplicity of simulation
    dsl.get_pipeline_conf().add_op_transformer(transformer_disable_caching)

    c = create_component_from_func(func=timed_sleep)

    # Simple sequential components
    s1 = c(seconds=base_time)
    s2 = c(seconds=base_time)
    s2.after(s1)
    s3 = c(seconds=base_time)
    s3.after(s2)

    # Parallel for components
    with dsl.ParallelFor([1, 2, 3, 5, 8]) as additional:
        p1 = c(seconds=base_time, additional=additional)
        p1.after(s3) # Run for loop after sequential components

    # Another sequential just to make the DAG not look terrible
    s4 = c(seconds=base_time)
    s4.after(p1)

    # Same thing again but with different parallelism
    with dsl.SubGraph(parallelism=15):
        with dsl.ParallelFor([1, 2, 3, 5, 8]) as additional:
            p2 = c(seconds=base_time, additional=additional)
            p2.after(s4)
