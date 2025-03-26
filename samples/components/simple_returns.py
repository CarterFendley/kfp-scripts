from typing import NamedTuple

def return_int(number: int) -> int:
    return number

def return_float(number: float) -> float:
    return number

def return_str(string: str) -> str:
    return string

def return_bool(boolean: bool) -> bool:
    return boolean

def return_dict(dictionary: dict) -> dict:
    return dictionary

def return_tuple(
    a: int,
    b: str,
    c: dict
) -> NamedTuple(
    'Output',
    [
        ("a", int),
        ("b", str),
        ("c", dict)
    ]
):
    return (a, b, c)