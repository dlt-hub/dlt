import importlib
import sys
from subprocess import check_call, CalledProcessError
from dlt.common.exceptions import MissingDependencyException


def install_missing_package(package):
    try:
        importlib.import_module(package)
    except ImportError:
        try:
            check_call(["pip", "install", package])
        except CalledProcessError as e:
            print(f"Could not install {package}")
            sys.exit(e.returncode)
    finally:
        globals()[package] = importlib.import_module(package)


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
            It must be "yes" (the default), "no" or None (meaning
            an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {
        "yes": True,
        "y": True,
        "ye": True,
        "no": False,
        "n": False,
    }
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        choice = input(f"{question} {prompt}").lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            print("Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")


def maybe_install_streamlit():
    try:
        import streamlit  # noqa
    except ModuleNotFoundError:
        yes_please = query_yes_no("Install streamlit")
        if yes_please:
            install_missing_package("streamlit")
        else:
            raise MissingDependencyException(
                "DLT Streamlit Helpers",
                ["streamlit"],
                "DLT Helpers for Streamlit should be run within a streamlit app.",
            )


maybe_install_streamlit()
