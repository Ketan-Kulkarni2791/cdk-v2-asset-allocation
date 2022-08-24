"""Script running pytest for config.ini."""
import os
import subprocess
import pytest

from script_utils import get_path_for_directory


def main() -> None:
    """Invoke 'pytest' shell command."""
    os.chdir(get_path_for_directory("test_config"))
    pytest.main()
    subprocess.check_output("pytest", shell=True, stderr=subprocess.STDOUT)
    

if __name__ == "__main__":
    main()