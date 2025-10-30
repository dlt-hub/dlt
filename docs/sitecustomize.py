import os, sys

# make tests folder available to the docs tests, not needed after we add a dlt pytest plugin
tests_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "tests"))
if os.path.isdir(tests_path) and tests_path not in sys.path:
    sys.path.insert(0, tests_path)
    