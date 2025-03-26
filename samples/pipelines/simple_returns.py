from kfp import dsl
from kfp.components import create_component_from_func

from samples.components import (
    return_int,
    return_float,
    return_str,
    return_bool,
    return_dict,
    return_tuple
)

def transformer_disable_caching(op):
    op.execution_options.caching_strategy.max_cache_staleness = "P0D"

def simple_returns():
    # Disable caching for simplicity of simulation
    dsl.get_pipeline_conf().add_op_transformer(transformer_disable_caching)

    c_return_int = create_component_from_func(func=return_int)
    c_return_float = create_component_from_func(func=return_float)
    c_return_str = create_component_from_func(func=return_str)
    c_return_bool = create_component_from_func(func=return_bool)
    c_return_dict = create_component_from_func(func=return_dict)
    c_return_tuple = create_component_from_func(func=return_tuple)

    # Simple sequential components
    s1 = c_return_str(string='Hello World!')
    s2 = c_return_bool(boolean=False)
    s2.after(s1)
    s3 = c_return_float(number=1.0)
    s3.after(s2)

    # Parallel for components
    with dsl.ParallelFor([2, 3]) as num:
        p1 = c_return_int(number=num)
        p1.after(s3) # Run for loop after sequential components

    s4 = c_return_dict(dictionary={'a':4, 'b': 'Hello Friend!'})
    s4.after(p1)
    s5 = c_return_tuple(
        a=5,
        b='Hello There!',
        c={'key': 'value'}
    )
    s5.after(s4)