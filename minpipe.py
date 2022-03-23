""""
A minimal library for building one-off concurrent data pipelines.
"""

import logging
import queue
from enum import Enum
from multiprocessing import Process, Queue
from threading import Thread
from typing import Any, Callable, Iterable, Optional, Union

__version__ = "0.1.0"

StageFunction = Union[Callable[[], Iterable[Any]], Callable[[Any], Iterable[Any]]]


class Stage:
    """
    A concurrent processing stage. The processing work is performed by a `StageFunction`
    `func` which accepts 0 or 1 inputs and returns an iterable of 0 or more outputs.
    Zero input stages are called initial stages and can be used for reading data from
    external sources. Initial stages should generate a `Signal.STOP` signal when
    finished.

    `name` is an optional name for the stage. By default, the name of `func` is used.
    `num_workers` sets the number of parallel workers used. When `num_workers` is 0,
    the stage runs in a `Thread` within the main process. Multiple workers are not
    supported for initial stages. `maxsize` is the maximum size for the stage's input
    queue. When `maxsize <= 0`, the input queue is unbounded.
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
        self._in = None
        self._out = None
        self._in_stage = None
        self._out_stage = None
        self._is_running = False

        if num_workers > 0:
            self._procs = [
                Process(target=self._worker, args=(rank,), daemon=True)
                for rank in range(num_workers)
            ]
        else:
            self._procs = [Thread(target=self._worker, daemon=True)]

    def pipe(self, other: "Stage"):
        """
        Pipe the stage's output to another stage.
        """
        assert not self._is_running, "no new pipes after starting"

        logging.info("New pipe: %s -> %s", self.name, other.name)
        # use an mp queue if communicating between processes
        mp = max(self.num_workers, other.num_workers) > 0
        self._out = other._new_channel(mp)
        self._out_stage = other
        other._in_stage = self

    def _new_channel(self, mp=False):
        assert not self._is_running, "no new pipes after starting"

        q = Queue(self.maxsize) if mp else queue.Queue(self.maxsize)
        self._in = q
        return q

    def _worker(self, rank: int = 0):
        logging.info("Starting %s/%d", self.name, rank)
        assert (
            rank == 0 or self._in is not None
        ), "multiple workers for initial stages are not supported"

        while True:
            if self._in is None:
                outputs = self.func()
            else:
                inpt = self._in.get()
                if inpt is Signal.STOP:
                    logging.info(
                        "Received stop signal in %s/%d; exiting", self.name, rank
                    )
                    if rank == 0:
                        self._send_stop()
                    return

                outputs = self.func(inpt)

            if outputs is not None:
                for output in outputs:
                    if output is Signal.STOP:
                        logging.info(
                            "Generated stop signal in %s/%d; exiting", self.name, rank
                        )
                        assert (
                            self._in is None
                        ), "only initial stages should generate stop signals"
                        if rank == 0:
                            self._send_stop()
                        return

                    if self._out is not None:
                        self._out.put(output)

    def _send_stop(self):
        if self._out is not None:
            for _ in range(max(self._out_stage.num_workers, 1)):
                self._out.put(Signal.STOP)

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
    A concurrent pipeline of sequential processing stages.
    """

    def __init__(self, *args: "Stage"):
        self.stages = list(args)

        for ii in range(len(self.stages) - 1):
            head, tail = self.stages[ii : ii + 2]
            head.pipe(tail)

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
            if stage._in is None:
                # initial stages run only once (empty args tuple)
                inputs = [()]
            else:
                parent_key = id(stage._in_stage)
                assert parent_key in dependencies, "pipeline stages should be in order"
                # convert to args tuple inpt -> (inpt,)
                inputs = zip(dependencies[parent_key])

            dependencies[id(stage)] = run(stage, inputs)

        results = [dependencies[id(stage)] for stage in self.stages]
        return results


class Signal(Enum):
    """
    Special signals for communicating between stages.
    """

    STOP = 1
