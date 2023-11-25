from argparse import ArgumentParser

from IPython.core.display import HTML, display
from IPython.core.magic import (
    Magics,
    cell_magic,
    line_cell_magic,
    line_magic,
    magics_class, register_line_magic,
)
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

@magics_class
class DltMagics(Magics):




    @magic_arguments()
    @argument("--line", "-s", type=str, help="Start date to render.")
    @register_line_magic
    @line_magic
    def reverse(self, line=None):
        """
        A simple IPython line magic command that reverses the input string.
        """
        # parser = ArgumentParser()
        # parser.add_argument("--line", "-s", type=str, help="Start date to render.")
        #
        # # Parse the arguments
        # args = parser.parse_args(line.split())
        return line[::-1]

    from init_command import init_command
    @magic_arguments()
    @argument("--line", "-s", type=str, help="Start date to render.")
    @register_line_magic
    @line_magic
    def init(self, line=None):
        """
        A DLT line magic command that initializes a DLT project.
        """
        parser = ArgumentParser()
        parser.add_argument("--line", "-s", type=str, help="Start date to render.")

        # Parse the arguments
        args = parser.parse_args(line.split())
        self.init_command(args)

        return
    # @property
    # def display(self) -> t.Callable:
    #     from sqlmesh import runtime_env
    #
    #     if runtime_env.is_databricks:
    #         # Use Databricks' special display instead of the normal IPython display
    #         return self._shell.user_ns["display"]
    #     return display

    # @property
    # def _context(self) -> Context:
    #     for variable_name in CONTEXT_VARIABLE_NAMES:
    #         context = self._shell.user_ns.get(variable_name)
    #         if context:
    #             return context
    #     raise MissingContextException(
    #         f"Context must be defined and initialized with one of these names: {', '.join(CONTEXT_VARIABLE_NAMES)}"
    #     )





def register_magics() -> None:
    from dlt.cli import echo as fmt
    fmt.echo("got here")
    try:
        fmt.echo("got here2")
        shell = get_ipython()  # type: ignore
        shell.register_magics(DltMagics)

        line_magics = shell.magics_manager.magics['line']
        fmt.echo("Registered Line Magics:" + str(line_magics))
    except NameError:
        pass



from ..common.runtime.exec_info import is_notebook
from dlt.cli import echo as fmt

def check_notebook_runtime():
    fmt.echo("Checking if notebook")
    if is_notebook():
        try:
            fmt.echo("Registering magics")
            register_magics()
        except ImportError:
            pass