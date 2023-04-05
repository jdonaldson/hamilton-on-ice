from typing import Any, Dict

import torch
from hamilton.function_modifiers import tag, extract_fields
from numpy import random

import pandas as pd


def training_data(batch: pd.DataFrame) -> pd.DataFrame:
    """Generates training data from the batch data"""
    return batch[batch["train"]].drop(["train", "eval"], axis=1).copy()


def evaluation_data(batch: pd.DataFrame) -> pd.DataFrame:
    """Generates evaluation data rom the batch data"""
    """This function takes the raw batch and returns the evaluation data"""
    return batch[batch["eval"]].drop(["train", "eval"], axis=1).copy()


@tag(feedback="validation_data", ice_equivalent="artifact")
def base_validation_data(training_data: pd.DataFrame) -> pd.DataFrame:
    """This is the 'base' validation data. The validation_data actually modifies this, and that
    value overrides it."""
    return training_data.copy()


def validation_data(
        training_data: pd.DataFrame,
        iteration: int,
        base_validation_data: pd.DataFrame) -> pd.DataFrame:
    """Validation data. Note that this modifies the "base validation data", which we override
    each time with the result of this. This is cause we do a modified version of reservoir
    sampling...

    @param training_data:
    @param iteration:
    @param base_validation_data:
    @return:
    """
    # I have an extreme aversion to mutating state so I keep copying stuff :)
    # Also this will do the swap on the first iteration versus the notebook which will not
    # That said, one could easily add an `iteration` input and change based off of that
    if iteration > 0:
        validation = base_validation_data.copy()
        val_idx = random.choice(validation.index)
        train_idx = random.choice(training_data.index)
        validation.loc[val_idx] = training_data.loc[train_idx]
        return validation
    return base_validation_data


@tag(feedback="output_model", ice_equivalent="artifact")
def model(training_data: pd.DataFrame) -> torch.nn.Sequential:
    """Basic function to create a model. This should only be called once."""
    n_features = training_data.drop(["survived"], axis=1).shape[1]
    model = torch.nn.Sequential(torch.nn.Linear(n_features, 50),
                                torch.nn.ReLU(),
                                torch.nn.Linear(50, 1),
                                torch.nn.Sigmoid())
    return model


@tag(feedback="criterion", ice_equivalent="artifact")
def criterion() -> torch.nn.Module:
    """Loss calculator, only created once (and cached).

    @return: The loss calculator
    """
    return torch.nn.BCELoss()


@tag(feedback="output_optimizer", ice_equivalent="artifact")
def optimizer(model: torch.nn.Sequential) -> torch.optim.Adam:
    """Optimizer, only created once and cached.

    @param model: Model we're using (so we know the parameters)
    @return: Optimizer (so we can update the state)
    """
    return torch.optim.Adam(model.parameters(), lr=0.0001, weight_decay=0.001)


@extract_fields(
    fields={"validation_loss": float,
            "loss": float,
            "output_model": torch.nn.Sequential,
            "output_optimizer": torch.optim.Adam})
def torch_train(
        training_data: pd.DataFrame,
        model: torch.nn.Sequential,
        optimizer: torch.optim.Adam,
        validation_data: pd.DataFrame,
        criterion: torch.nn.Module) -> Dict[str, Any]:
    """Trains the model using pytorch. Note this is stateless -- it accepts the model and the
    optimizer, and outputs them again under different names. This way, we can loop back the output
    into the input.

    @param training_data: Training data to use for this batch.
    @param model: Model (partially trained) to use
    @param optimizer: Optimizer (stateful) to use
    @param validation_data: Data to use for validation
    @param criterion: Loss function calculator
    @return: A dictionary consisting of everything we need from this model
    """
    optimizer.zero_grad()
    train = training_data.drop(["survived"], axis=1).to_numpy()
    train_features = torch.tensor(train)
    train_outputs = model(train_features.float())
    train_labels = torch.tensor(training_data["survived"].to_numpy())

    validation = validation_data.drop(["survived"], axis=1).to_numpy()
    validation_features = torch.tensor(validation)
    validation_outputs = model(validation_features.float())
    validation_labels = torch.tensor(validation_data["survived"].to_numpy())
    loss = criterion(
        train_outputs.flatten().float(),
        train_labels.flatten().float()
    )

    validation_loss = criterion(
        validation_outputs.flatten().float(),
        validation_labels.flatten().float()
    )

    loss.backward()
    optimizer.step()
    return {
        'validation_loss': float(validation_loss),
        'loss': float(loss),
        'output_model': model,
        'output_optimizer': optimizer
    }
