"""Prepare litestar openapi spec for Runtime API python client generation"""
import copy
import sys
import yaml


def construct_error_schema_name(status_code: str) -> str:
    return f"ErrorResponse{status_code}"

def construct_error_schema_ref(status_code: str) -> dict:
    return {"$ref": f"#/components/schemas/{construct_error_schema_name(status_code)}"}


def extract_postprocess_error_response_schemas(spec: dict) -> dict:
    """
    Deduplicate error response schemas and replace them with references to the deduplicated schemas, 
    fix the extra type to be an object for all 400 responses
    """
    error_response_schemas = {}
    error_schemas_original_paths = {}
    result_spec = copy.deepcopy(spec)
    
    for path, path_content in spec["paths"].items():
        for method, method_content in path_content.items():
            for status_code, response in method_content["responses"].items():
                if int(status_code) < 400:
                    continue
                schema = response['content']['application/json']['schema']
                result_spec_pointer = result_spec["paths"][path][method]["responses"][status_code]['content'] \
                    ['application/json']
                if existing_schema := error_response_schemas.get(status_code):
                    if existing_schema != schema:
                        existing_schema_path = error_schemas_original_paths[status_code]
                        print(
                            f"Error response schema for status code {status_code} in {path} "
                            f"is different from {existing_schema_path}, using the original schema", 
                            file=sys.stderr
                        )
                    else:
                        result_spec_pointer['schema'] = construct_error_schema_ref(status_code)
                    continue
                error_response_schemas[status_code] = schema
                error_schemas_original_paths[status_code] = path
                result_spec_pointer['schema'] = construct_error_schema_ref(status_code)
    
    for value in error_response_schemas.values():
        value['properties']['extra']['type'] = "object"
    
    for status_code, schema in error_response_schemas.items():
        result_spec["components"]["schemas"][construct_error_schema_name(status_code)] = schema

    return result_spec


if __name__ == "__main__":
    file_path = sys.argv[1]
    output_file_path = sys.argv[2] if len(sys.argv) > 2 else file_path
    with open(file_path, "r") as f:
        spec = yaml.safe_load(f)

    spec = extract_postprocess_error_response_schemas(spec)

    with open(output_file_path, "w") as f:
        yaml.dump(spec, f)
