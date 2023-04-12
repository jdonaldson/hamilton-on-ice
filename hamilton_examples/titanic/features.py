from typing import Dict

import numpy as np
import pandas as pd
from hamilton.function_modifiers import extract_columns, inject, does, config


def survived(survived_raw: pd.Series) -> pd.Series:
    return survived_raw.astype(float)


def norm_pclass(pclass: pd.Series) -> pd.Series:
    return pclass / 3


def male_sex(sex: pd.Series) -> pd.Series:
    return (sex == "male").astype(float)


def female_sex(sex: pd.Series) -> pd.Series:
    return (sex == "female").astype(float)


def norm_age(age: pd.Series) -> pd.Series:
    age_normalized = (age / 100)
    mean_value = age_normalized.mean(skipna=True)
    age_normalized.fillna(mean_value, inplace=True)
    return age_normalized


def norm_sib_sp(sibsp: pd.Series) -> pd.Series:
    return sibsp / 10


def norm_parch(parch: pd.Series) -> pd.Series:
    return parch / 10


def norm_fare(fare: pd.Series) -> pd.Series:
    return fare / 1000


def embarked_c(embarked: pd.Series) -> pd.Series:
    return (embarked == "C").astype(float)


def embarked_s(embarked: pd.Series) -> pd.Series:
    return (embarked == "S").astype(float)


def embarked_q(embarked: pd.Series) -> pd.Series:
    return (embarked == "Q").astype(float)


@extract_columns("train", "eval")
def test_train_split(batch_raw: pd.DataFrame, random_seed: int,
                     fraction_train_data: float = 0.8) -> pd.DataFrame:
    # np.random.seed(random_seed)
    msk = np.random.rand(len(batch_raw)) < fraction_train_data
    return pd.DataFrame(index=batch_raw.index,
                        data={
                            "train": msk,
                            "eval": ~msk,
                        })


def _form_dataframe(**kwargs: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(kwargs)


@does(_form_dataframe)
@config.when(fine_grained_lineage=True)
def batch__fine_grained(
        survived: pd.Series,
        norm_pclass: pd.Series,
        male_sex: pd.Series,
        female_sex: pd.Series,
        norm_age: pd.Series,
        norm_sib_sp: pd.Series,
        norm_parch: pd.Series,
        norm_fare: pd.Series,
        embarked_c: pd.Series,
        embarked_s: pd.Series,
        embarked_q: pd.Series,
        train: pd.Series,
        eval: pd.Series,
) -> pd.DataFrame:
    """We tend to prefer fine-grained lineage so you can test each one individually. This just
    groups it all together. Note that you can also use the @inject to remove the unecessary parameters."""
    pass


@config.when_not(fine_grained_lineage=True)
def batch__coarse_grained(batch_raw: pd.DataFrame, test_train_split: pd.DataFrame) -> pd.DataFrame:
    """This is a nice way to do it if you don't care as much about lineage. Just copied your
    code over! DAG will look smaller."""
    normalized = pd.DataFrame({
        "survived": batch_raw["survived"].astype(float),
        "norm_pclass": batch_raw["pclass"] / 3,
        "male_sex": (batch_raw["sex"] == "male").astype(float),
        "female_sex": (batch_raw["sex"] == "female").astype(float),
        "norm_age": batch_raw["age"] / 100,
        "norm_sibsp": batch_raw["sibsp"] / 10,
        "norm_parch": batch_raw["parch"] / 10,
        "norm_fare": batch_raw["fare"] / 1000,
        "embarked_c": (batch_raw["embarked"] == "C").astype(float),
        "embarked_s": (batch_raw["embarked"] == "S").astype(float),
        "embarked_q": (batch_raw["embarked"] == "Q").astype(float),
        "train": test_train_split["train"],
        "eval": test_train_split["eval"],
    })
    mean_value = normalized["norm_age"].mean(skipna=True)
    normalized["norm_age"].fillna(mean_value, inplace=True)
    return normalized
