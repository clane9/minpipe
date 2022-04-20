# minpipe

minpipe is a minimal library for building one-off concurrent data pipelines in python. It was inspired by [pypeln](https://github.com/cgarciae/pypeln).

## Example

```python
import numpy as np
from minpipe import Pipeline, Signal, Stage

def make_random_data():
    x = np.random.randn(10, 10, 10000)

    def random_data():
        for xi in x:
            yield xi
        yield Signal.STOP

    return random_data

def mean(xi):
    yield np.mean(xi, axis=-1)

def flatten(means):
    for m in means:
        yield m

def filter_pos(m):
    if m > 0:
        yield m

def printout(m):
    print(f"mean={m:.2f}")

pipeline = Pipeline(
    Stage(make_random_data()),
    Stage(mean, num_workers=2),
    Stage(flatten, num_workers=1),
    Stage(filter_pos),
    Stage(printout),
)

# serial debug run
# pipeline.serial(max_items=10)

pipeline.start()
pipeline.join()
```

## Installation

```
pip install minpipe
```

## See also

- [pypeln](https://github.com/cgarciae/pypeln)
- [bonobo](https://www.bonobo-project.org/)

## Licence

MIT
