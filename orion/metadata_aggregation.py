from __future__ import annotations

from collections import OrderedDict
from typing import Any

from orion.metadata_transforms import evaluate_transform


class AggregationEngine:
    def __init__(self, aggregate_spec: dict[str, Any]):
        self.aggregate_spec = aggregate_spec
        self.groups: "OrderedDict[tuple[Any, ...], dict[str, Any]]" = OrderedDict()

    def consume_row(self, row: dict[str, Any]) -> None:
        items = evaluate_transform(self.aggregate_spec["foreach"], row=row)
        for item in items:
            group_key = tuple(
                evaluate_transform(key_spec, row=row, item=item)
                for key_spec in self.aggregate_spec.get("group_by", [])
            )
            state = self.groups.setdefault(group_key, {})
            for reducer_name, reducer_spec in self.aggregate_spec.get("reducers", {}).items():
                self._apply_reducer(state, reducer_name, reducer_spec, row=row, item=item)

    def _apply_reducer(
        self,
        state: dict[str, Any],
        reducer_name: str,
        reducer_spec: dict[str, Any],
        row: dict[str, Any],
        item: Any,
    ) -> None:
        reducer_op = reducer_spec["op"]

        if reducer_op == "collect_list":
            value = evaluate_transform(reducer_spec["value"], row=row, item=item)
            if value is None:
                return
            state.setdefault(reducer_name, []).append(value)
            return

        if reducer_op == "collect_unique":
            value = evaluate_transform(reducer_spec["value"], row=row, item=item)
            if value is None:
                return
            values = state.setdefault(reducer_name, [])
            if value not in values:
                values.append(value)
            return

        if reducer_op == "count":
            state[reducer_name] = state.get(reducer_name, 0) + 1
            return

        if reducer_op == "first_non_null":
            if reducer_name in state and state[reducer_name] is not None:
                return
            value = evaluate_transform(reducer_spec["value"], row=row, item=item)
            if value is not None:
                state[reducer_name] = value
            return

        raise ValueError(f"Unsupported aggregation reducer: {reducer_op}")

    def finalize(self) -> list[tuple[tuple[Any, ...], dict[str, Any]]]:
        finalized_groups: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        for group_key, state in self.groups.items():
            aggregate = dict(state)
            for final_name, final_spec in self.aggregate_spec.get("finalize", {}).items():
                aggregate[final_name] = evaluate_transform(
                    final_spec,
                    row={},
                    aggregate=aggregate,
                    group_key=group_key,
                )
            finalized_groups.append((group_key, aggregate))
        return finalized_groups
