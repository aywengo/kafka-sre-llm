"""A small JSON Schema checker for tool arguments (the subset the tool schemas use; standard library only).

Supported: type object/string/integer/boolean, properties, required, additionalProperties false, enum, pattern
(full match), minimum, maximum, maxLength. Returns a list of (kind, path, message), where kind is one of
"type", "required", "extra", "enum", "pattern", "range". The allow-list decides which kinds are violations.
"""
import re

_TYPES = {"object": dict, "string": str, "boolean": bool, "array": list}


def _is_type(value, t):
    if t == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if t == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    return isinstance(value, _TYPES[t])


def validate(value, schema, path="arguments"):
    errors = []
    t = schema.get("type")
    if t and not _is_type(value, t):
        return [("type", path, f"expected {t}, got {type(value).__name__}")]
    if t == "object":
        props = schema.get("properties", {})
        for name in schema.get("required", []):
            if name not in value:
                errors.append(("required", f"{path}.{name}", "required"))
        for name, v in value.items():
            if name not in props:
                if schema.get("additionalProperties", True) is False:
                    errors.append(("extra", f"{path}.{name}", "not an argument of this tool"))
                continue
            errors += validate(v, props[name], f"{path}.{name}")
        return errors
    if "enum" in schema and value not in schema["enum"]:
        errors.append(("enum", path, f"{value!r} is not one of the allowed values"))
    if isinstance(value, str):
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            errors.append(("range", path, f"longer than {schema['maxLength']} characters"))
        if "pattern" in schema and not re.fullmatch(schema["pattern"], value):
            errors.append(("pattern", path, f"{value!r} does not match {schema['pattern']}"))
    if _is_type(value, "number"):
        if "minimum" in schema and value < schema["minimum"]:
            errors.append(("range", path, f"below {schema['minimum']}"))
        if "maximum" in schema and value > schema["maximum"]:
            errors.append(("range", path, f"above {schema['maximum']}"))
    return errors
