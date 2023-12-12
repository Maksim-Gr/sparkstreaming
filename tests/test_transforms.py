import pytest
from pandas.testing import assert_frame_equal

pytestmark = pytest.mark.usefixtures("spark")
