import yaml
import os
import argparse
import click

from dlt.common.logger import dlt_version
from dlt.common import json
from dlt.common.schema import Schema
from dlt.common.typing import DictStrAny

from dlt.pipeline import attach

import dlt.cli.echo as fmt
from dlt.cli import utils
from dlt.cli.init_command import init_command
from dlt.cli.deploy_command import PipelineWasNotRun, deploy_command
from dlt.pipeline.exceptions import CannotRestorePipelineException


# def add_pool_cli_arguments(parser: argparse.ArgumentParser) -> None:
#     parser.add_argument("--is-single-run", action="store_true", help="exit when all pending items are processed")
#     parser.add_argument("--wait-runs", type=int, nargs='?', const=True, default=1, help="maximum idle runs to wait for incoming data")


# def str2bool_a(v: str) -> bool:
#     try:
#         return str2bool(v)
#     except ValueError:
#         raise argparse.ArgumentTypeError('Boolean value expected.')


def init_command_wrapper(pipeline_name: str, destination_name: str, use_generic_template: bool, branch: str) -> None:
    try:
        init_command(pipeline_name, destination_name, use_generic_template, branch)
    except Exception as ex:
        click.secho(str(ex), err=True, fg="red")
        raise
        exit(-1)
        # TODO: display stack trace if with debug flag


def deploy_command_wrapper(pipeline_script_path: str, deployment_method: str, schedule: str, run_on_push: bool, run_on_dispatch: bool, branch: str) -> None:
    try:
        utils.ensure_git_command("deploy")
    except Exception as ex:
        click.secho(str(ex), err=True, fg="red")
        exit(-1)

    from git import InvalidGitRepositoryError, NoSuchPathError
    try:
        deploy_command(pipeline_script_path, deployment_method, schedule, run_on_push, run_on_dispatch, branch)
        return
    except (CannotRestorePipelineException, PipelineWasNotRun) as ex:
        click.secho(str(ex), err=True, fg="red")
        click.echo("Currently you must run the pipeline at least once")
    except InvalidGitRepositoryError:
        click.secho(
            "No git repository found for pipeline script %s. Add your local code to Github as described here: %s" % (fmt.bold(pipeline_script_path), fmt.bold("https://docs.github.com/en/get-started/importing-your-projects-to-github/importing-source-code-to-github/adding-locally-hosted-code-to-github")),
            err=True,
            fg="red"
        )
    except NoSuchPathError as path_ex:
       click.secho(
            "The pipeline script does not exist\n%s" % str(path_ex),
            err=True,
            fg="red"
        )
    except Exception as ex:
        click.secho(str(ex), err=True, fg="red")
        # TODO: display stack trace if with debug flag
        raise
    exit(-1)

def main() -> None:
    parser = argparse.ArgumentParser(description="Runs various DLT modules", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--version', action='version', version='%(prog)s {version}'.format(version=dlt_version()))
    subparsers = parser.add_subparsers(dest="command")

    init_cmd = subparsers.add_parser("init", help="Creates a new pipeline script from a selected template.")
    init_cmd.add_argument("source", help="Data source name. If pipeline for given data source already exists it will be used as a template. Otherwise new template will be created.")
    init_cmd.add_argument("destination", help="Name of a destination ie. bigquery or redshift")
    init_cmd.add_argument("--generic", default=False, action="store_true", help="When present uses a generic template with all the dlt loading code present will be used. Otherwise a debug template is used that can be immediately run to get familiar with the dlt sources.")
    init_cmd.add_argument("--branch", default=None, help="Advanced. Uses specific branch of the init repository to fetch the template.")

    deploy_cmd = subparsers.add_parser("deploy", help="Creates a deployment package for a selected pipeline script")
    deploy_cmd.add_argument("pipeline_script_path", help="Path to a pipeline script")
    deploy_cmd.add_argument("deployment_method", choices=["github-action"], default="github-action", help="Deployment method")
    deploy_cmd.add_argument("--schedule", required=True, help="A schedule with which to run the pipeline, in cron format. Example: '*/30 * * * *' will run the pipeline every 30 minutes.")
    deploy_cmd.add_argument("--run-manually", default=True, action="store_true", help="Allows the pipeline to be run manually form Github Actions UI.")
    deploy_cmd.add_argument("--run-on-push", default=False, action="store_true", help="Runs the pipeline with every push to the repository.")
    deploy_cmd.add_argument("--branch", default=None, help="Advanced. Uses specific branch of the deploy repository to fetch the template.")

    schema = subparsers.add_parser("schema", help="Shows, converts and upgrades schemas")
    schema.add_argument("file", help="Schema file name, in yaml or json format, will autodetect based on extension")
    schema.add_argument("--format", choices=["json", "yaml"], default="yaml", help="Display schema in this format")
    schema.add_argument("--remove-defaults", action="store_true", help="Does not show default hint values")

    pipe_cmd = subparsers.add_parser("pipeline", help="Operations on the pipelines")
    pipe_cmd.add_argument("name", help="Pipeline name")
    pipe_cmd.add_argument("operation", choices=["failed_loads", "drop", "init"], default="failed_loads", help="Show failed loads for a pipeline")
    pipe_cmd.add_argument("--workdir", help="Pipeline working directory", default=None)

    args = parser.parse_args()

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

        p = attach(pipeline_name=args.name, pipelines_dir=args.workdir)
        print(f"Found pipeline {p.pipeline_name} ({args.name}) in {p.pipelines_dir} ({args.workdir}) with state {p._get_state()}")

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
        init_command_wrapper(args.source, args.destination, args.generic, args.branch)
        exit(0)
    elif args.command == "deploy":
        deploy_command_wrapper(args.pipeline_script_path, args.deployment_method, args.schedule, args.run_on_push, args.run_manually, args.branch)
        exit(0)
    else:
        parser.print_help()
        exit(-1)

    # run_args = TRunnerArgs(args.is_single_run, args.wait_runs)
    # run_f(run_args)

if __name__ == "__main__":
    main()
