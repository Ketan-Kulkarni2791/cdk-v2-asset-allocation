"""Script running pytest for CDK constructs."""
import os
import subprocess
import pytest

from script_utils import get_path_for_directory


def main() -> None:
    """Invoke 'pytest' shell command."""
    
    covreport = "AWS-Infrastructure-Coverage-Report.xml"
    junitxml = "AWS-Infrastructure-Test-Result.xml"
    os.chdir(get_path_for_directory("test_cdk_unit"))
    pytest.main()
    subprocess.check_output(
        "pip install pytest pytest-cov", shell=True, stderr=subprocess.STDOUT
    )
    subprocess.check_output(
        f"python -m pytest --cov --cov-report=xml:{covreport} --junitxml={junitxml}",
        shell=True,
        stderr=subprocess.STDOUT
    )


if __name__ == "__main__":
    main()