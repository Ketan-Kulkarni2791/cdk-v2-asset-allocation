"""Script for running mypy type hint checks."""
import subprocess

from script_utils import get_path_for_file, ROOT_DIR


def main() -> None:
    """Invoke 'mypy' shell command."""
    print(f"--------- root dir : {ROOT_DIR}")
    subprocess.run(
        f"mypy {ROOT_DIR} --config-file {get_path_for_file('mypy.ini')}",
        cwd=ROOT_DIR,
        check=True,
        shell=True
    )


if __name__ == "__main__":
    main()