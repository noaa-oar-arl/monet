import numpy as np

from monet.util.stats import scores


def test_scores():
    # Due to our low bias, we miss one
    obs = np.linspace(0, 1, 21)
    mod = obs - 0.1  # low bias
    a, b, c, d = scores(obs, mod, minval=0.5)
    assert a == 9 and b == 1 and c == 0 and d == 11

    # All good
    a, b, c, d = scores([1, 1], [1, 1], minval=0)
    assert a == 2 and b == 0 and c == 0 and d == 0

    # All miss
    a, b, c, d = scores([1, 1], [-1, -1], minval=0)
    assert a == 0 and b == 2 and c == 0 and d == 0

    # All false alarm
    a, b, c, d = scores([-1, -1], [1, 1], minval=0)
    assert a == 0 and b == 0 and c == 2 and d == 0

    # All correct negative
    a, b, c, d = scores([-1, -1], [-1, -1], minval=0)
    assert a == 0 and b == 0 and c == 0 and d == 2

    # Same but mix
    a, b, c, d = scores([-1, 1], [-1, 1], minval=0)
    assert a == 1 and b == 0 and c == 0 and d == 1
    a, b, c, d = scores([1, -1], [1, -1], minval=0)
    assert a == 1 and b == 0 and c == 0 and d == 1

    # Opposite
    a, b, c, d = scores([1, -1], [-1, 1], minval=0)
    assert a == 0 and b == 1 and c == 1 and d == 0
    a, b, c, d = scores([-1, 1], [1, -1], minval=0)
    assert a == 0 and b == 1 and c == 1 and d == 0
