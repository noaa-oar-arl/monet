import numpy as np
from monet.plots.taylordiagram import TaylorDiagram


def test_taylor_diagram_basic():
    # Create a TaylorDiagram with a reference std
    refstd = 1.0
    td = TaylorDiagram(refstd)
    assert hasattr(td, "ax")
    assert hasattr(td, "refstd")
    assert np.isclose(td.refstd, refstd)


def test_taylor_diagram_add_sample():
    refstd = 1.0
    td = TaylorDiagram(refstd)
    # Add a sample with stddev and corrcoef
    stddev = 1.2
    corrcoef = 0.8
    sample = td.add_sample(stddev, corrcoef, marker="o", label="Test")
    assert sample is not None


def test_taylor_diagram_add_contours():
    refstd = 1.0
    td = TaylorDiagram(refstd)
    # Add contours to the diagram
    contours = td.add_contours(levels=3)
    assert contours is not None
