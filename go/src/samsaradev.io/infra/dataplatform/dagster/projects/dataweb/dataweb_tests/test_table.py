import pytest
from dagster._check import CheckError
from dataweb._core import _checks, table


@pytest.mark.parametrize(
    "invalid_input", [["invalid-region"], [], ["us-west-2", "another-invalid-region"]]
)
def test_check_regions_input__raises_error__on_invalid_region(invalid_input):
    with pytest.raises(CheckError):
        _checks._check_regions_input("asset", invalid_input)


@pytest.mark.parametrize("valid_input", [["us-west-2"], ["us-west-2", "eu-west-1"]])
def test_check_regions_input__succeeds__on_valid_regions(valid_input):
    _checks._check_regions_input("asset", valid_input)
