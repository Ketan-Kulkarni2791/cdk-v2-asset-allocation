"""This is the script to install the dependencies needed for the lambdas to run."""
import logging
import os
from typing import AnyStr, List

logging.getLogger().setLevel(logging.INFO)

ROOT_DIR = os.getcwd()
logging.info("Starting script in %s", ROOT_DIR)


# noinspection PyTypeChecker
def get_src_directory():
    """Get relative path to /src location."""
    # pylint: disable=inconsistent-return-statements,unused-variable
    logging.info("Getting path to /src location.")
    for dirname, dirnames, _ in os.walk(ROOT_DIR):
        for source_dir in dirnames:
            if source_dir == "src":
                return os.path.join(dirname, source_dir)
    return None


def get_lambda_directories(top_level_dir: bytes) -> List[AnyStr]:
    """Get a list of all directories in src."""
    logging.info("Putting together list of directories that need unit testing.")
    dir_paths = []
    for subdir in os.scandir(top_level_dir):
        dir_paths.append(os.path.abspath(subdir))
    return dir_paths


# noinspection PyTypeChecker
def install_packages(directory_path):
    """Installing packages required for lambda."""
    logging.info("Begginning installation activities for %s", directory_path.rsplit('/', 1)[-1])
    # Navigate to the directory
    logging.info("Changing working directory to %s", directory_path)
    os.chdir(directory_path)
    logging.info("Installing dependencies for deployment.")
    os.system("python -m pip install -r requirements-app.txt -t .")


# noinspection PyTypeChecker
def main() -> None:
    """Top level method for installing packages required by lambda layers."""
    src_directory = get_src_directory()
    lambda_directories = get_lambda_directories(src_directory)

    for lambda_dir in lambda_directories:
        install_packages(lambda_dir)


if __name__ == "__main__":
    main()    
