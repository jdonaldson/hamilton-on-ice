from typing import Generator, Tuple, Dict, Any

import pandas as pd
from hamilton.function_modifiers import load_from, value, extract_columns, tag, config, \
    extract_fields


@tag(feedback="minibatch_data_generator")
def minibatch_data_generator(batch_size: int) -> Generator[Tuple[pd.DataFrame, int], None, None]:
    """This is an infinite looping generator.
    The cleaner way to do this is to make this loop through once and have the HamiltonOnIce
    class know when it should stop -- but for now this is clean enough.

    @param batch_size: Size of the batch to use.
    @return:
    """
    iteration = 0
    while True:
        for chunk in pd.read_csv(
                "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv",
                chunksize=batch_size):
            chunk.columns = [item.lower() for item in chunk.columns]
            chunk["survived_raw"] = chunk["survived"]
            yield chunk, iteration
        iteration += 1


@extract_fields(
    fields={
        "batch_raw": pd.DataFrame,
        "batch_number": int
    }
)
@config.when_not(batch_size=None)
def minibatch_data(
        minibatch_data_generator: Generator[Tuple[pd.DataFrame, int], None, None]) -> Dict[str, Any]:
    """This exists solely to call next() on the generator, and return the data.
    We then split it out into two

    @param minibatch_data_generator: Generator for the data
    @return: The next chunk of data
    """
    data, iteration = next(minibatch_data_generator)
    return {
        "batch_raw": data,
        "batch_number": iteration
    }


@extract_columns(
    "survived_raw",
    "pclass",
    "sex",
    "age",
    "sibsp",
    "parch",
    "fare",
    "embarked",
)
def batch_raw_for_extract(batch_raw: pd.DataFrame) -> pd.DataFrame:
    """This only exists to use the extract_columns which, currently, doesn't work alongside
    `extract_fields`

    @param batch_raw: Batch of raw data
    @return:
    """
    return batch_raw
