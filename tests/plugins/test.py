from importlib.metadata import entry_points
from importlib import import_module

print("start")

for entry_point in entry_points().select(group="dlt.plugin"):
    import_module(entry_point.value)