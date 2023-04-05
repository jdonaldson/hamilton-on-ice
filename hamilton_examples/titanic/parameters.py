"""Note that these don't have to be their own functions, there are a few ways to inject this data"""


def observed_size() -> int:
    return 0


def reservoir_size() -> int:
    return 50


def random_seed() -> int:
    return 42


def output_field() -> str:
    return "Survived"
