import yaml
import os
import argparse
import click
from typing import Callable

from dlt.common import json
from dlt.common.schema import Schema
from dlt.common.typing import DictStrAny

from dlt.pipeline import attach

from dlt.cli import TRunnerArgs
from dlt.cli.init_command import init_command


def add_pool_cli_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--is-single-run", action="store_true", help="exit when all pending items are processed")
    parser.add_argument("--wait-runs", type=int, nargs='?', const=True, default=1, help="maximum idle runs to wait for incoming data")


# def str2bool_a(v: str) -> bool:
#     try:
#         return str2bool(v)
#     except ValueError:
#         raise argparse.ArgumentTypeError('Boolean value expected.')


def init_command_wrapper(pipeline_name: str, destination_name: str, branch: str) -> None:
    try:
        init_command(pipeline_name, destination_name, branch)
    except Exception as ex:
        click.secho(str(ex), err=True, fg="red")
        # TODO: display stack trace if with debug flag


def main() -> None:
    parser = argparse.ArgumentParser(description="Runs various DLT modules", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(dest="command")

    # dbt = subparsers.add_parser("dbt", help="Executes dbt package")
    # add_pool_cli_arguments(dbt)
    init_cmd = subparsers.add_parser("init", help="Creates new pipeline script from a selected template.")
    init_cmd.add_argument("pipeline_name", help="Pipeline name. If pipeline with given name already exists it will be used as a template. Otherwise new template will be created.")
    init_cmd.add_argument("destination_name", help="Name of the destination ie. bigquery or redshift")
    init_cmd.add_argument("--branch", default=None, help="Advanced. Uses specific branch of the init repository to fetch the template.")

    schema = subparsers.add_parser("schema", help="Shows, converts and upgrades schemas")
    schema.add_argument("file", help="Schema file name, in yaml or json format, will autodetect based on extension")
    schema.add_argument("--format", choices=["json", "yaml"], default="yaml", help="Display schema in this format")
    schema.add_argument("--remove-defaults", action="store_true", help="Does not show default hint values")

    pipe_cmd = subparsers.add_parser("pipeline", help="Operations on the pipelines")
    pipe_cmd.add_argument("name", help="Pipeline name")
    pipe_cmd.add_argument("operation", choices=["failed_loads", "drop", "init"], default="failed_loads", help="Show failed loads for a pipeline")
    pipe_cmd.add_argument("--workdir", help="Pipeline working directory", default=None)

    args = parser.parse_args()
    # run_f: Callable[[TRunnerArgs], None] = None

    # if args.command == "normalize":
    #     from dlt.normalize.normalize import run_main as normalize_run
    #     run_f = normalize_run
    # elif args.command == "load":
    #     from dlt.load.load import run_main as loader_run
    #     run_f = loader_run
    # if args.command == "dbt":
    #     from dlt.dbt_runner.runner import run_main as dbt_run
    #     run_f = dbt_run
    if args.command == "schema":
        with open(args.file, "r", encoding="utf-8") as f:
            if os.path.splitext(args.file)[1][1:] == "json":
                schema_dict: DictStrAny = json.load(f)
            else:
                schema_dict = yaml.safe_load(f)
        s = Schema.from_dict(schema_dict)
        if args.format == "json":
            schema_str = json.dumps(s.to_dict(remove_defaults=args.remove_defaults), indent=2)
        else:
            schema_str = s.to_pretty_yaml(remove_defaults=args.remove_defaults)
        print(schema_str)
        exit(0)
    elif args.command == "pipeline":

        p = attach(pipeline_name=args.name, working_dir=args.workdir)
        print(f"Found pipeline {p.pipeline_name} ({args.name}) in {p.working_dir} ({args.workdir}) with state {p._get_state()}")

        if args.operation == "failed_loads":
            completed_loads = p.list_completed_load_packages()
            for load_id in completed_loads:
                print(f"Checking failed jobs in load id '{load_id}'")
                for job, failed_message in p.list_failed_jobs_in_package(load_id):
                    print(f"JOB: {os.path.abspath(job)}\nMSG: {failed_message}")

        if args.operation == "drop":
            p.drop()

        exit(0)
    elif args.command == "init":
        init_command_wrapper(args.pipeline_name, args.destination_name, args.branch)
        exit(0)
    else:
        parser.print_help()
        exit(-1)

    # run_args = TRunnerArgs(args.is_single_run, args.wait_runs)
    # run_f(run_args)

if __name__ == "__main__":
    main()
