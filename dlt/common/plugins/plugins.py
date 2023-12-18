

class Plugin:
    
    NAME: str = None

    def __init__(self):
        pass

    #
    # contracts
    #
    def on_schema_contract_violation(self, violation: str):
        pass

PLUGINS: list[Plugin] = []

def register_plugin(plugin: Plugin):
    PLUGINS.append(plugin)