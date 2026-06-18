from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any, Iterable


_SINGLE_REFERENCE_PATTERN = re.compile(r"^\$([A-Za-z_][\w.]*)$")
_TEMPLATE_REFERENCE_PATTERN = re.compile(r"\$\{([A-Za-z_][\w.]*)\}")
_EXPAND_PATTERN = re.compile(r"^\s*([A-Za-z_][\w]*)\s+as\s+([A-Za-z_][\w]*)\s*$")

_SCALAR_FIELD_KINDS = {
    "identifier",
    "optional_identifier",
    "label",
    "property",
}


@dataclass(frozen=True)
class SchemaField:
    kind: str
    children: dict[str, "SchemaField"] = field(default_factory=dict)


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value == "":
        return True
    if isinstance(value, (list, tuple, dict, set)) and len(value) == 0:
        return True
    return False


def apply_prefix(value: Any, prefix: str | None) -> Any:
    if prefix is None or is_missing(value):
        return value
    normalized_prefix = str(prefix).rstrip(":")
    normalized = str(value).strip()
    if normalized.startswith(f"{normalized_prefix}:"):
        return normalized
    return f"{normalized_prefix}:{normalized}"


def normalize_atomic_value(raw_value: Any, spec: dict[str, Any]) -> Any:
    if raw_value is None:
        return None

    trim = spec.get("trim", True)
    value = str(raw_value).strip() if trim else str(raw_value)
    if value == "":
        return "" if spec.get("preserve_empty", False) else None

    kind = spec.get("kind", "property")
    if kind in {"identifier", "optional_identifier"}:
        return apply_prefix(value, spec.get("prefix"))
    return value


def _split_parts(raw_value: Any, separator: str, trim: bool = True) -> list[str]:
    if is_missing(raw_value):
        return []
    parts = []
    for part in str(raw_value).split(separator):
        normalized = part.strip() if trim else part
        if normalized == "":
            continue
        parts.append(normalized)
    return parts


def normalize_field(raw_row: dict[str, Any], field_spec: dict[str, Any]) -> Any:
    kind = field_spec.get("kind", "property")

    if kind in _SCALAR_FIELD_KINDS:
        return normalize_atomic_value(raw_row.get(field_spec["column"]), field_spec)

    if kind == "list":
        parts = _split_parts(
            raw_row.get(field_spec["column"]),
            field_spec.get("separator", "|"),
            trim=field_spec.get("trim", True),
        )
        prefix = field_spec.get("prefix")
        if prefix is not None:
            return [apply_prefix(part, prefix) for part in parts]
        return parts

    if kind == "zipped_list":
        separator = field_spec.get("separator", "|")
        sub_specs = field_spec.get("columns", {})
        split_values: dict[str, list[str]] = {}
        for sub_name, sub_spec in sub_specs.items():
            split_values[sub_name] = _split_parts(
                raw_row.get(sub_spec["column"]),
                separator,
                trim=sub_spec.get("trim", True),
            )

        lengths = {len(values) for values in split_values.values()}
        if not lengths or lengths == {0}:
            return []
        if len(lengths) != 1:
            raise ValueError(
                f"zipped_list field has mismatched lengths for columns "
                f"{list(sub_specs.keys())}: {sorted(lengths)}"
            )

        row_count = next(iter(lengths))
        records: list[dict[str, Any]] = []
        for index in range(row_count):
            record: dict[str, Any] = {}
            for sub_name, sub_spec in sub_specs.items():
                record[sub_name] = normalize_atomic_value(split_values[sub_name][index], sub_spec)
            records.append(record)
        return records

    if kind == "value_columns":
        values: list[dict[str, Any]] = []
        for column_name, column_spec in field_spec.get("columns", {}).items():
            raw_value = raw_row.get(column_name)
            if is_missing(raw_value):
                continue
            normalized_value = str(raw_value).strip()
            if normalized_value == "":
                continue

            item = {
                "value": normalized_value,
                "column": column_name,
            }
            for key, value in column_spec.items():
                item[key] = value
            values.append(item)
        return values

    raise ValueError(f"Unsupported semantic field kind: {kind}")


def resolve_reference(data: dict[str, Any], path: str) -> Any:
    current: Any = data
    for part in path.split("."):
        if isinstance(current, dict):
            if part not in current:
                return None
            current = current[part]
            continue
        if isinstance(current, (list, tuple)) and part.isdigit():
            current = current[int(part)]
            continue
        return None
    return current


def render_template(template: str, row: dict[str, Any]) -> str:
    def replace(match: re.Match[str]) -> str:
        value = resolve_reference(row, match.group(1))
        return "" if value is None else str(value)

    return _TEMPLATE_REFERENCE_PATTERN.sub(replace, template)


def _evaluate_parse_qualified_float(spec: dict[str, Any], row: dict[str, Any]) -> float | None:
    value = evaluate_expression(spec["value"], row)
    if is_missing(value):
        return None

    if isinstance(value, (int, float)):
        parsed_value = float(value)
    else:
        normalized = str(value).strip().replace(",", "")
        for operator in spec.get("reject_operators", []):
            if normalized.startswith(operator):
                return None
        for operator in spec.get("strip_operators", ["<"]):
            if normalized.startswith(operator):
                normalized = normalized[len(operator):]
        parsed_value = float(normalized)

    minimum_exclusive = spec.get("minimum_exclusive")
    if minimum_exclusive is not None and parsed_value <= float(minimum_exclusive):
        return None

    minimum_inclusive = spec.get("minimum_inclusive")
    if minimum_inclusive is not None and parsed_value < float(minimum_inclusive):
        return None

    return parsed_value


def _evaluate_neglog10_nm(spec: dict[str, Any], row: dict[str, Any]) -> float | None:
    value = evaluate_expression(spec["value"], row)
    if is_missing(value):
        return None

    numeric_value = float(value)
    if numeric_value <= 0:
        return None

    transformed = -(math.log10(numeric_value * (10 ** -9)))
    if "precision" in spec:
        return round(transformed, int(spec["precision"]))
    return transformed


def evaluate_expression(spec: Any, row: dict[str, Any]) -> Any:
    if spec is None:
        return None

    if isinstance(spec, list):
        return [evaluate_expression(item, row) for item in spec]

    if isinstance(spec, str):
        reference_match = _SINGLE_REFERENCE_PATTERN.match(spec)
        if reference_match:
            return resolve_reference(row, reference_match.group(1))
        if "${" in spec:
            return render_template(spec, row)
        return spec

    if isinstance(spec, dict):
        if "parse_qualified_float" in spec:
            return _evaluate_parse_qualified_float(spec["parse_qualified_float"], row)
        if "neglog10_nm" in spec:
            return _evaluate_neglog10_nm(spec["neglog10_nm"], row)
        return {
            key: evaluate_expression(value, row)
            for key, value in spec.items()
        }

    return spec


def matches_filter(filter_spec: dict[str, Any], row: dict[str, Any]) -> bool:
    if "exists" in filter_spec:
        return not is_missing(resolve_reference(row, filter_spec["exists"]))

    if "not_exists" in filter_spec:
        return is_missing(resolve_reference(row, filter_spec["not_exists"]))

    if "equals" in filter_spec:
        condition = filter_spec["equals"]
        return evaluate_expression(condition["value"], row) == resolve_reference(row, condition["field"])

    if "not_equals" in filter_spec:
        condition = filter_spec["not_equals"]
        return evaluate_expression(condition["value"], row) != resolve_reference(row, condition["field"])

    if "non_empty" in filter_spec:
        value = resolve_reference(row, filter_spec["non_empty"])
        return not is_missing(value)

    raise ValueError(f"Unsupported semantic-table filter: {filter_spec}")


def matches_filters(filter_specs: Iterable[dict[str, Any]], row: dict[str, Any]) -> bool:
    return all(matches_filter(filter_spec, row) for filter_spec in filter_specs)


def parse_expand_spec(spec: str | dict[str, Any] | None, operation_name: str) -> tuple[str, str] | None:
    if spec is None:
        return None

    if isinstance(spec, str):
        match = _EXPAND_PATTERN.match(spec)
        if not match:
            raise ValueError(
                f"{operation_name} must use '<field> as <name>' syntax or a mapping with field/as keys."
            )
        return match.group(1), match.group(2)

    if isinstance(spec, dict):
        return spec["field"], spec["as"]

    raise ValueError(f"Unsupported {operation_name} specification: {spec!r}")


def iter_expression_references(spec: Any) -> Iterable[str]:
    if spec is None:
        return

    if isinstance(spec, str):
        reference_match = _SINGLE_REFERENCE_PATTERN.match(spec)
        if reference_match:
            yield reference_match.group(1)
        for match in _TEMPLATE_REFERENCE_PATTERN.finditer(spec):
            yield match.group(1)
        return

    if isinstance(spec, list):
        for item in spec:
            yield from iter_expression_references(item)
        return

    if isinstance(spec, dict):
        for value in spec.values():
            yield from iter_expression_references(value)


def schema_for_field_spec(field_spec: dict[str, Any]) -> SchemaField:
    kind = field_spec.get("kind", "property")
    if kind in _SCALAR_FIELD_KINDS:
        return SchemaField(kind="scalar")

    if kind == "list":
        return SchemaField(kind="list")

    if kind == "zipped_list":
        return SchemaField(
            kind="record_list",
            children={
                name: SchemaField(kind="scalar")
                for name in field_spec.get("columns", {}).keys()
            },
        )

    if kind == "value_columns":
        child_names = {"value", "column"}
        for column_spec in field_spec.get("columns", {}).values():
            child_names.update(column_spec.keys())
        return SchemaField(
            kind="record_list",
            children={name: SchemaField(kind="scalar") for name in sorted(child_names)},
        )

    raise ValueError(f"Unsupported semantic field kind: {kind}")


def resolve_schema_path(schema: dict[str, SchemaField], path: str) -> SchemaField:
    current = schema.get(path.split(".")[0])
    if current is None:
        raise KeyError(f"Unknown reference '{path}'")

    for part in path.split(".")[1:]:
        if part not in current.children:
            raise KeyError(f"Unknown reference '{path}'")
        current = current.children[part]

    return current


def infer_expression_schema(spec: Any, schema: dict[str, SchemaField]) -> SchemaField:
    if isinstance(spec, str):
        reference_match = _SINGLE_REFERENCE_PATTERN.match(spec)
        if reference_match:
            return resolve_schema_path(schema, reference_match.group(1))
        if "${" in spec:
            return SchemaField(kind="scalar")
        return SchemaField(kind="scalar")

    if isinstance(spec, list):
        return SchemaField(kind="list")

    if isinstance(spec, dict):
        if "parse_qualified_float" in spec or "neglog10_nm" in spec:
            return SchemaField(kind="scalar")
        return SchemaField(kind="scalar")

    return SchemaField(kind="scalar")
