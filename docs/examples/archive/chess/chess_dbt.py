import os
import dlt

# from chess import chess

if __name__ == "__main__":
    print("You must run this from the docs/examples/chess folder")
    assert os.getcwd().endswith("chess")
    print("Make sure the pipeline in chess.py was ran at least once")

    # chess_url in config.toml, credentials for postgres in secrets.toml, credentials always under credentials key
    pipeline = dlt.attach(pipeline_name="chess_games")
    # create or restore exiting virtual environment where dbt exists
    venv = dlt.dbt.get_venv(pipeline, dbt_version="1.2.4")
    # get the runner for the "dbt_transform" package
    transforms = dlt.dbt.package(pipeline, "dbt_transform", venv=venv)
    # run all the steps (deps -> seed -> source tests -> run)
    # request all the source tests
    models = transforms.run_all(source_tests_selector="source:*")
    print(models)
    # run all the tests
    tests = transforms.test()
    print(tests)
