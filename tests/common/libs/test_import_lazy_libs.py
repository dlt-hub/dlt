import subprocess
import sys


def test_import_dlt_does_not_load_optional_libs() -> None:
    """Bare `import dlt` must not pull optional dataframe/typing libs into sys.modules.

    Run in a subprocess with `-I` so pytest's own imports don't taint the result.
    """
    code = (
        "import sys, dlt\n"
        "heavy = ['pyarrow', 'pandas', 'numpy', 'polars', 'pydantic', 'requests', 'ibis']\n"
        "loaded = sorted(m for m in heavy if m in sys.modules)\n"
        "assert not loaded, f'leaked: {loaded}'\n"
    )
    result = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
