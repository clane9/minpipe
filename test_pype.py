import logging

import numpy as np

from pype import Sequential, Signal, Stage

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


def test_sequential():
    np.random.seed(2022)

    def make_random_data():
        x = np.random.randn(10, 10, 10000)

        def random_data():
            for ii, xi in enumerate(x):
                yield ii, xi
            yield Signal.STOP

        return random_data

    def mean(inpt):
        ii, xi = inpt
        yield ii, np.mean(xi, axis=-1)

    def flatten(inpt):
        ii, ms = inpt
        for jj, m in enumerate(ms):
            yield ii, jj, m

    def filter(inpt):
        ii, jj, m = inpt
        if m > 0:
            yield ii, jj, m

    def printout(inpt):
        ii, jj, m = inpt
        logging.info(f"({ii},{jj}) mean={m:.2f}")

    pipeline = Sequential(
        Stage(make_random_data()),
        Stage(mean, num_workers=2),
        Stage(flatten, num_workers=1),
        Stage(filter, num_workers=1),
        Stage(printout, num_workers=0),
    )

    logging.info("Short serial test run")
    pipeline.serial(max_items=10)

    logging.info("Full serial test run")
    pipeline.serial()

    logging.info("Concurrent run")
    pipeline.start()
    pipeline.join()
