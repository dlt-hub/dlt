"""description of the workspace visible in runtime"""

__tags__ = ["production", "team:data-eng"]

# batch jobs
from job_runs import backfil_chats, analyze_chats, get_daily_chats, load_chats, job_picker

# will deploy marimo notebook
import notebook

# will deploy mcp
import my_mcp

# will deploy streamlit
import my_streamlit_app

# will deploy custom uwsgi
from my_uwsgi import start

# only names in __all__ are discovered as jobs
__all__ = [
    "backfil_chats",
    "analyze_chats",
    "get_daily_chats",
    "load_chats",
    "job_picker",
    "notebook",
    "my_mcp",
    "my_streamlit_app",
    "start",
]
