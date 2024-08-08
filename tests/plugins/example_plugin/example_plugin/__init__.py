import dlt
import dlt.plugins

from argparse import ArgumentParser, Namespace


@dlt.plugins.cli_plugin(subcommand="generate-stuff", cli_help="Let's generate some stuff")
def register_dlt_plugin(parser: ArgumentParser = None, namespace: Namespace = None) -> None:
    # setup command
    if parser:
        parser.description = "HELLO"
        parser.add_argument(
            "--example-flag",
            action="store_true",
            help="This is how we add example flags.",
            default=False,
        )

    # execute stuff
    if namespace:
        print("Doing some stuff")
        print(namespace.example_flag)
