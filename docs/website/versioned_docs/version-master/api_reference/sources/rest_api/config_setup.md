---
sidebar_label: config_setup
title: sources.rest_api.config_setup
---

## create\_response\_hooks

```python
def create_response_hooks(
    response_actions: Optional[List[ResponseAction]]
) -> Optional[Dict[str, Any]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/sources/rest_api/config_setup.py#L536)

Create response hooks based on the provided response actions. Note
that if the error status code is not handled by the response actions,
the default behavior is to raise an HTTP error.

**Example**:

  def set_encoding(response, *args, **kwargs):
  response.encoding = 'windows-1252'
  return response
  
  def remove_field(response: Response, *args, **kwargs) -> Response:
  payload = response.json()
  for record in payload:
  record.pop("email", None)
- `modified_content` - bytes = json.dumps(payload).encode("utf-8")
  response._content = modified_content
  return response
  
  response_actions = [
  set_encoding,
- `{"status_code"` - 404, "action": "ignore"},
- `{"content"` - "Not found", "action": "ignore"},
- `{"status_code"` - 200, "content": "some text", "action": "ignore"},
- `{"status_code"` - 200, "action": remove_field},
  ]
  hooks = create_response_hooks(response_actions)

