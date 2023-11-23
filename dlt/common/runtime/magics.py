
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

    @register_line_magic
    def reverse(self, line):
        """
        A simple IPython line magic command that reverses the input string.
        """
        return line[::-1]
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
    try:
        shell = get_ipython()  # type: ignore
        shell.register_magics(DltMagics)
    except NameError:
        pass