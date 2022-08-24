"""Script for running Prospector checks on non-test files."""
import subprocess

from script_utils import get_path_for_file, ROOT_DIR


def main() -> None:
    """Invoke 'prospector' shell command on non-test files."""
    subprocess.run(
        f"prospector {ROOT_DIR} --profile {get_path_for_file('prospector.yml')}",
        cwd=ROOT_DIR,
        check=True,
        shell=True
    )


if __name__ == "__main__":
    main()