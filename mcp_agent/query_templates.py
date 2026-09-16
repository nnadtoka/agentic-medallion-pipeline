"""Validate and resolve `run_supported_query` parameters against a
template's stored spec.

Kept separate from `mcp_agent/server.py` so this -- the part that actually
enforces the safety guardrails described in `mcp_agent/README.md` -- can be
unit tested without a live Postgres connection or the MCP protocol.

Two kinds of parameter, and only two ways a parameter can influence the
query that actually runs:

- `value` -- validated (type/range/enum), then bound as a normal psycopg2
  named parameter (`%(name)s`) at execution time. Safe by construction
  regardless of content.
- `identifier` -- the agent can only pick from a fixed `enum` mapping
  declared in the template's own stored row (e.g. `{"customer_state":
  "c.customer_state"}`). The *value* the agent supplies must be a key in
  that mapping; the SQL fragment actually spliced into the template via
  `str.format()` is the mapped value, which is never agent-supplied text.
  If the agent's value isn't a declared key, this raises -- there is no
  fallback to raw text.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


class QueryTemplateError(ValueError):
    """Raised when a template lookup or parameter fails validation."""


def resolve_query(
    sql_template: str,
    parameter_specs: list[dict],
    params: dict[str, Any],
    max_limit: int,
) -> tuple[str, dict[str, Any]]:
    """Return `(sql, bind_params)` ready for `cursor.execute(sql, bind_params)`.

    Raises `QueryTemplateError` on any missing required parameter, unknown
    identifier value, or value failing its declared type/range/enum check.
    A `limit` parameter (if the template declares one) is always capped at
    `max_limit`, regardless of what the caller asked for -- defense in
    depth on top of the per-parameter validation below.
    """

    bind_params: dict[str, Any] = {}
    identifier_fragments: dict[str, str] = {}
    has_limit_param = False

    for spec in parameter_specs:
        name = spec["name"]
        kind = spec["kind"]
        required = bool(spec.get("required", False))
        supplied = name in params and params[name] is not None

        if kind == "identifier":
            if not supplied:
                if required:
                    raise QueryTemplateError(f"Missing required parameter: {name!r}")
                continue
            enum_map = spec.get("enum") or {}
            value = params[name]
            if value not in enum_map:
                allowed = ", ".join(sorted(enum_map))
                raise QueryTemplateError(
                    f"{name!r} must be one of: {allowed} (got {value!r})"
                )
            identifier_fragments[name] = enum_map[value]

        elif kind == "value":
            if name == "limit":
                has_limit_param = True
            if not supplied:
                if required:
                    raise QueryTemplateError(f"Missing required parameter: {name!r}")
                bind_params[name] = spec.get("default")
                continue
            bind_params[name] = _validate_value(spec, params[name])

        else:
            raise QueryTemplateError(f"Unknown parameter kind {kind!r} for {name!r}")

    if has_limit_param:
        requested = bind_params.get("limit")
        bind_params["limit"] = (
            max_limit if requested is None else min(int(requested), max_limit)
        )

    sql = sql_template.format(**identifier_fragments) if identifier_fragments else sql_template
    return sql, bind_params


def _validate_value(spec: dict, value: Any) -> Any:
    name = spec["name"]
    value_type = spec.get("type", "string")

    if value_type == "integer":
        try:
            value = int(value)
        except (TypeError, ValueError):
            raise QueryTemplateError(f"{name!r} must be an integer, got {value!r}") from None
    elif value_type == "number":
        try:
            value = float(value)
        except (TypeError, ValueError):
            raise QueryTemplateError(f"{name!r} must be a number, got {value!r}") from None
    elif value_type == "date":
        if not isinstance(value, str):
            raise QueryTemplateError(f"{name!r} must be an ISO date string, got {value!r}")
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise QueryTemplateError(f"{name!r} must be YYYY-MM-DD, got {value!r}") from None
    elif value_type == "string":
        if not isinstance(value, str):
            raise QueryTemplateError(f"{name!r} must be a string, got {value!r}")
    else:
        raise QueryTemplateError(f"Unknown parameter type {value_type!r} for {name!r}")

    minimum = spec.get("min")
    maximum = spec.get("max")
    if minimum is not None and value < minimum:
        raise QueryTemplateError(f"{name!r} must be >= {minimum}, got {value!r}")
    if maximum is not None and value > maximum:
        raise QueryTemplateError(f"{name!r} must be <= {maximum}, got {value!r}")

    enum = spec.get("enum")
    if enum is not None and value not in enum:
        raise QueryTemplateError(f"{name!r} must be one of {sorted(enum)}, got {value!r}")

    return value
