"""Test ShareSight support."""

from pathlib import Path
import subprocess
import sys

import pytest

@pytest.mark.parametrize("test_case", [
    "with_asterisks",
    "without_asterisks",
])
def test_run_with_sharesight_files_no_balance_check(test_case: str, request: pytest.FixtureRequest) -> None:
    """Runs the script and verifies it doesn't fail."""
    cmd = [
        sys.executable,
        "-m",
        "cgt_calc.main",
        "--year",
        "2020",
        "--sharesight",
        f"tests/test_data/sharesight/{test_case}",
        "--no-pdflatex",
        "--no-balance-check",
    ]
    result = subprocess.run(cmd, check=True, capture_output=True)
    assert result.stderr == b"", "Run with example files generated errors"

    test_name = "test_run_with_sharesight_files_no_balance_check"
    expected_file = (
        Path("tests")
        / "test_data"
        / f"{test_name}_{test_case}_output.txt"
    )
    expected = expected_file.read_text()
    cmd_str = " ".join([param if param else "''" for param in cmd])
    assert result.stdout.decode("utf-8") == expected, (
        "Run with example files generated unexpected outputs, "
        "if you added new features update the test with:\n"
        f"{cmd_str} > {expected_file}"
    )
