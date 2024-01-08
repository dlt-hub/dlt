from dlt.common.exceptions import DltException


class PluginException(DltException):
    pass


class UnknownPluginPathException(PluginException):
    def __init__(self, plugin_path: str) -> None:
        self.plugin_path = plugin_path
        msg = f"Plugin at path {plugin_path} could not be found and imported"
        super().__init__(msg)
