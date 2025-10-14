"""Prepare litestar openapi spec for python client generation"""

import sys
import yaml


if __name__ == "__main__":
    file_path = sys.argv[1]
    with open(file_path, "r") as f:
        spec = yaml.safe_load(f)

    for path in spec["paths"]:
        for method in spec["paths"][path]:
            for response in spec["paths"][path][method]["responses"]:
                if int(response) >= 400:
                    # we need to fix the extra type to be an object for all 400 responses, this might break
                    spec["paths"][path][method]["responses"][response]['content']['application/json']['schema']['properties']['extra']['type'] = "object"


    with open(file_path, "w") as f:
        yaml.dump(spec, f)