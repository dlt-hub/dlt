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



# def is_notebook() -> bool:
#     try:
#         # Import get_ipython from IPython
#         from IPython import get_ipython
#
#         # Get the class name of the current IPython instance
#         shell = get_ipython().__class__.__name__
#
#         # Check if the shell is a Jupyter notebook (ZMQInteractiveShell) and return as boolean
#         return shell == 'ZMQInteractiveShell'
#     except NameError:
#         # Return False if get_ipython() is not available, indicating a non-IPython environment
#         return False
#
# print(is_notebook())

# source_name='chess', destination_name='duckdb', use_generic_template=True, repo_location='https://github.com/dlt-hub/verified-sources')


# Example of running a magic command
oo = shell.run_line_magic('init',"--source_name=chess --destination_name=duckdb --use_generic_template --repo_location=https://github.com/dlt-hub/verified-sources" )

print(oo)

# ff = shell.run_line_magic('dlt_version', '')
# print(ff)

#init_command(source_name='chess', destination_name='duckdb', use_generic_template=True, repo_location='https://github.com/dlt-hub/verified-sources')
