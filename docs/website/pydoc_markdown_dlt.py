from pydoc_markdown.contrib.processors.smart import SmartProcessor
import re
from functools import partial

sub = partial(re.sub, flags=re.M)


class DltProcessor(SmartProcessor):
    def _process(self, node):
        if not getattr(node, "docstring", None):
            return

        # join long lines ending in escape (\)
        c = sub(r"\\\n\s*", "", node.docstring.content)
        # remove markdown headers
        c = sub(r"^#### (.*?)$", r"\1", c)
        # convert REPL code blocks to code
        c = sub(r"^(\s*>>>|\.\.\.)(.*?)$", r"```\n\1\2\n```", c)
        c = sub(r"^(\s*>>>|\.\.\.)(.*?)\n```\n```\n(\s*>>>|\.\.\.)", r"\1\2\n\3", c)
        c = sub(r"^(\s*>>>|\.\.\.)(.*?)\n```\n```\n(\s*>>>|\.\.\.)", r"\1\2\n\3", c)
        c = sub(r"^(\s*```)(\n\s*>>>) ", r"\1py\2", c)
        c = sub(r"(\n\s*)(>>> ?)", r"\1", c)
        node.docstring.content = c

        return super()._process(node)
