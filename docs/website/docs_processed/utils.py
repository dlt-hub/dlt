import tomlkit
from typing import List

DLT_MARKER = "@@@DLT_"


def parse_toml_file(filename: str) -> None:
    """Test TOML file by splitting into snippets and parsing them separately"""
    with open(filename, "r", encoding="utf-8") as f:
        # use whitespace preserving parser
        lines = f.readlines()

        current_lines: List[str] = []
        current_marker = ""
        for line in lines:
            if DLT_MARKER in line:
                toml_snippet = "\n".join(current_lines)
                try:
                    tomlkit.loads(toml_snippet)
                except Exception as e:
                    print(
                        f"Error while testing snippet between: {current_marker} and {line.strip()}"
                    )
                    raise e
                current_lines = []
                current_marker = line.strip()
            current_lines.append(line)
