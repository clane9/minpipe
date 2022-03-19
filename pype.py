""""
A minimal library for building one-off concurrent data pipelines.
"""

import logging
from enum import Enum
from multiprocessing import Lock, Process, Queue
from threading import Thread
from typing import Any, Callable, Iterable, List, Optional, Union

__version__ = "0.1.0"

StageFunction = Callable[..., Iterable[Any]]
StageGroup = Union["Stage", List["Stage"]]


class Stage:
    """
    A concurrent processing stage. The processing work is performed by a `StageFunction`
    `func` which operates on a group of 0 or more inputs and returns an iterable of
    outputs. For a given group of inputs, the function may generate 0 or more outputs,
    enabling filtering, mapping, and flat mapping. Zero input stages are called initial
    stages and can be used for reading data from external sources. Initial stages should
    generate a `Signal.STOP` signal when finished.

    `name` is an optional name for the stage. By default, the name of `func` is used.
    `num_workers` sets the number of parallel workers used. When `num_workers` is 0,
    the stage runs in a `Thread` within the main process. `maxsize` is the maximum size
    for the stage's input queue(s). When `maxsize <= 0`, the input queue is unbounded.
    """

    def __init__(
        self,
        func: StageFunction,
        name: Optional[str] = None,
        num_workers: int = 0,
        maxsize: int = 0,
    ):
        self.func = func
        self.name = name if name is not None else func.__name__
        self.num_workers = num_workers
        self.maxsize = maxsize
        self._in = []
        self._out = []
        self._in_stages = []
        self._out_stages = []
        self._is_running = False

        if num_workers > 0:
            self._procs = [
                Process(target=self._worker, args=(rank,), daemon=True)
                for rank in range(num_workers)
            ]
        else:
            self._procs = [Thread(target=self._worker, daemon=True)]
        self._lock = Lock()

    def pipe(self, other: StageGroup):
        """
        Pipe the stage's output to another stage or group of stages.
        """
        assert not self._is_running, "no new pipes after starting"

        if isinstance(other, Stage):
            other = [other]

        for stage in other:
            logging.info("New pipe: %s -> %s", self.name, stage.name)
            q = stage._new_channel()
            self._out.append(q)
            self._out_stages.append(stage)
            stage._in_stages.append(self)

    def _new_channel(self):
        assert not self._is_running, "no new pipes after starting"
        q = Queue(self.maxsize)
        self._in.append(q)
        return q

    def _worker(self, rank: int = 0):
        logging.info("Starting %s/%d", self.name, rank)
        assert (
            rank == 0 or len(self._in) > 0
        ), "multiple workers for initial stages are not supported"

        while True:
            # avoid a race that could cause workers to get inconsistent items
            # from multiple inputs
            with self._lock:
                inputs = [q.get() for q in self._in]
            closed = [inp is Signal.STOP for inp in inputs]
            if any(closed):
                logging.info("Received stop signal in %s/%d; exiting", self.name, rank)
                assert all(closed), "some but not all input queues are closed"
                if rank == 0:
                    self._send_stop()
                return

            outputs = self.func(*inputs)
            if outputs is not None:
                for output in outputs:
                    if output is Signal.STOP:
                        logging.info(
                            "Generated stop signal in %s/%d; exiting", self.name, rank
                        )
                        assert (
                            len(self._in) == 0
                        ), "only initial stages should generate stop signals"
                        if rank == 0:
                            self._send_stop()
                        return

                    for q in self._out:
                        q.put(output)

    def _send_stop(self):
        for q, stage in zip(self._out, self._out_stages):
            for _ in range(max(stage.num_workers, 1)):
                q.put(Signal.STOP)

    def start(self):
        """
        Start stage.
        """
        self._is_running = True
        for p in self._procs:
            p.start()

    def join(self):
        """
        Wait for stage to finish.
        """
        for p in self._procs:
            p.join()
        self._is_running = False


class Pipeline:
    """
    A concurrent pipeline of processing stages.
    """

    def __init__(self, stages: List["Stage"]):
        self.stages = stages

    def start(self):
        """
        Start pipeline.
        """
        for stage in self.stages:
            stage.start()

    def join(self):
        """
        Wait for pipeline to finish.
        """
        for stage in self.stages:
            stage.join()

    def serial(self, max_items: int = -1):
        """
        Run pipeline serially, e.g. for debugging. Returns a list of stage results.
        `max_items` is the maximum items produced in each stage. When `max_items <= 0`,
        the entire pipeline is run. Note that initial stages are run only once.
        """
        dependencies = {}

        # run a stage on a sequence of inputs
        def run(stage, inputs):
            cache = []
            for inpt in inputs:
                outputs = stage.func(*inpt)
                if outputs is not None:
                    for output in outputs:
                        if output is Signal.STOP:
                            return cache
                        cache.append(output)
                        if len(cache) >= max_items > 0:
                            return cache
            return cache

        for stage in self.stages:
            inputs = []
            for parent in stage._in_stages:
                # using memory location as unique key for each stage
                parent_key = id(parent)
                assert parent_key in dependencies, "pipeline stages must be in order"
                inputs.append(dependencies[parent_key])

            # note, initial stages are called only once
            inputs = zip(*inputs) if len(inputs) > 0 else [()]
            dependencies[id(stage)] = run(stage, inputs)

        results = [dependencies[id(stage)] for stage in self.stages]
        return results


class Sequential(Pipeline):
    """
    A pipeline consisting of a sequence of stages.
    """

    def __init__(self, *args: "Stage"):
        stages = list(args)

        for ii in range(len(stages) - 1):
            head, tail = stages[ii : ii + 2]
            head.pipe(tail)

        super().__init__(stages)


class Signal(Enum):
    """
    Special signals for communicating between stages.
    """

    STOP = 1
