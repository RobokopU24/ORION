from __future__ import annotations

import re
from typing import Any


_TEMPLATE_PATTERN = re.compile(r"\{([^}]+)\}")


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value == "":
        return True
    return False


def _resolve_item_value(item: Any, token: str) -> Any:
    current = item
    for part in token.split("."):
        if isinstance(current, (list, tuple)):
            current = current[int(part)]
        elif isinstance(current, dict):
            current = current.get(part)
        else:
            raise ValueError(f"Cannot traverse item token '{token}' on value {current!r}")
    return current


def _resolve_template_token(token: str, row: dict[str, Any], item: Any = None) -> Any:
    if token.startswith("item."):
        if item is None:
            raise ValueError(f"Template token '{token}' requested without a foreach item.")
        return _resolve_item_value(item, token[len("item."):])
    return row.get(token)


def render_template(template: str, row: dict[str, Any], item: Any = None) -> str:
    def replace(match: re.Match[str]) -> str:
        token = match.group(1)
        value = _resolve_template_token(token, row=row, item=item)
        return "" if value is None else str(value)

    return _TEMPLATE_PATTERN.sub(replace, template)


def evaluate_transform(spec: Any, row: dict[str, Any], item: Any = None) -> Any:
    if isinstance(spec, list):
        return [evaluate_transform(value, row=row, item=item) for value in spec]

    if not isinstance(spec, dict) or "op" not in spec:
        return spec

    op = spec["op"]

    if op == "literal":
        return spec.get("value")

    if op == "field":
        value = row.get(spec["name"])
        if _is_missing(value):
            return spec.get("default")
        return value

    if op == "item":
        if item is None:
            raise ValueError("The item transform requires a foreach context.")
        if "path" in spec:
            return _resolve_item_value(item, str(spec["path"]))
        if "index" in spec:
            return _resolve_item_value(item, str(spec["index"]))
        raise ValueError("The item transform requires either 'path' or 'index'.")

    if op == "template":
        return render_template(spec["value"], row=row, item=item)

    if op == "coalesce":
        for value_spec in spec.get("values", []):
            value = evaluate_transform(value_spec, row=row, item=item)
            if not _is_missing(value):
                return value
        return None

    if op == "prefix":
        value = evaluate_transform(spec["value"], row=row, item=item)
        if _is_missing(value):
            return None
        return f'{spec["prefix"]}{value}'

    if op == "prefix_if_present":
        field_value = row.get(spec["field"])
        if _is_missing(field_value):
            return None
        return f'{spec["prefix"]}{field_value}'

    if op == "split":
        value = evaluate_transform(spec["value"], row=row, item=item)
        if _is_missing(value):
            return []
        return [part for part in str(value).split(spec.get("separator", "|")) if part]

    if op == "split_prefix":
        field_value = row.get(spec["field"])
        if _is_missing(field_value):
            return []
        separator = spec.get("separator", "|")
        prefix = spec["prefix"]
        return [f"{prefix}{part}" for part in str(field_value).split(separator) if part]

    if op == "explode_zip":
        separator = spec.get("separator", "|")
        split_values: list[list[str]] = []
        for field_name in spec.get("fields", []):
            raw_value = row.get(field_name)
            if _is_missing(raw_value):
                return []
            split_values.append([part for part in str(raw_value).split(separator) if part])

        lengths = {len(parts) for parts in split_values}
        if len(lengths) > 1:
            raise ValueError(
                f"explode_zip fields have mismatched lengths for fields {spec.get('fields')}: {lengths}"
            )
        return list(zip(*split_values))

    if op == "map_lookup":
        value = evaluate_transform(spec["value"], row=row, item=item)
        return spec.get("mapping", {}).get(value, spec.get("default"))

    raise ValueError(f"Unsupported metadata transform: {op}")


def row_matches_filter(filter_spec: dict[str, Any], row: dict[str, Any]) -> bool:
    if "exists" in filter_spec:
        return not _is_missing(row.get(filter_spec["exists"]))
    if "not_exists" in filter_spec:
        return _is_missing(row.get(filter_spec["not_exists"]))
    if "equals" in filter_spec:
        condition = filter_spec["equals"]
        return row.get(condition["field"]) == condition["value"]
    if "not_equals" in filter_spec:
        condition = filter_spec["not_equals"]
        return row.get(condition["field"]) != condition["value"]
    raise ValueError(f"Unsupported row filter: {filter_spec}")

