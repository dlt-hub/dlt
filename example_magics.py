import dlt
# from dlt.cli.init_command import init_command
#
# from dlt.helpers.magics import register_magics
from dlt.cli import echo as fmt
from IPython import get_ipython

from IPython.terminal.interactiveshell import InteractiveShell
# register_magics()

shell = get_ipython()

fmt.echo("got here234234")


# source_name='chess', destination_name='duckdb', use_generic_template=True, repo_location='https://github.com/dlt-hub/verified-sources')


# Example of running a magic command
oo = shell.run_line_magic('init',"--source_name=chess --destination_name=duckdb --use_generic_template --repo_location=https://github.com/dlt-hub/verified-sources" )

print(oo)

# ff = shell.run_line_magic('dlt_version', '')
# print(ff)

#init_command(source_name='chess', destination_name='duckdb', use_generic_template=True, repo_location='https://github.com/dlt-hub/verified-sources')
