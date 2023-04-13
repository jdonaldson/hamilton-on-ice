from hamilton import driver
from typing import List, Dict, Any
import collections
# This is the basic prototype of a stateful hamilton DAG
# Note that this generates data that just repeats and updates state
# It does not *yet* know about streaming inputs, although I think we can do that.
# This is basically a state machine, where the output of the last run gets fed into the
# input of the next one
class HamiltonOnIce:
    def __init__(self,
                 driver: driver.Driver,
                 remember: List[str],
                 outputs: List[str],
                 inputs: Dict[str, Any]):
        """Initializes a Hamilton on Ice driver. This calls a hamilton DAG in a
        sequential chain, in which the state from the prior run is passed into
        the inputs to the current run as overrides.

        We know which nodes to pass in due to the value of the tag `feedback`.
        For example,

        @tag(feedback="output_optimizer")
        def optimizer(...) -> ...:
            ...

        means that `optimizer` will only be called the first time. On subsequent
        loads, the prior value of `output_optimizer` will be called.

        Each HamiltonOnIce instance stores the state in itself, so you'll want to create a
        new one to iterate through again.

        :param driver: hamilton driver to use
        :param remember: list of items to store after finishing -- these will be accessible in the `memory`
            property
        :param inputs: constant list of inputs to the DAG
        :param outputs: outputs we finally care about in the end, but don't necessarily want to remember
        """
        self._driver = driver
        self._feedback_nodes = {
            var.name: var.tags["feedback"] for var in self._driver.list_available_variables() if "feedback" in var.tags
        }
        self._memory = []
        self._remember = remember
        self._inputs = inputs
        self._outputs = outputs
        self._feedback = {}
        self._iteration = 0

    @property
    def memory(self) -> Dict[str, List[Any]]:
        """Gives the memory out as a dict of node names -> list(values)"""
        output = collections.defaultdict(list)
        for state in self._memory:
            for key, value in state.items():
                output[key].append(value)
        return output

    def __iter__(self):
        """Need to decide how to reset state/whanot, this works for now..."""
        return self

    def __next__(self):
        """Single iteration of a hamilton run"""
        vars_ = set([*self._remember, *self._outputs, *sorted(self._feedback_nodes.values())])
        result = self._driver.execute(
            vars_, # We execute the vars we care about
            inputs={**self._inputs, "iteration": self._iteration},
            overrides=self._feedback,
        )
        self._iteration += 1
        self._feedback = {key: result[value] for key, value in self._feedback_nodes.items()}
        self._memory.append({key: result[key] for key in self._remember}) # Remember the memory
        return {key: result[key] for key in self._outputs} # return the output
