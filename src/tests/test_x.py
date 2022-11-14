"""
Initial pytest file for testing the data fusion module.
"""

# imports
import pytest

# project-based imports
from src.fusion import batch_fusion

# test function
def func(x):
    return x + 1

# initial test class
class TestClass:
    @pytest.mark.parametrize("x", [0, 1, 2])
    def test_x(self, x):
        assert func(x) == x + 1
