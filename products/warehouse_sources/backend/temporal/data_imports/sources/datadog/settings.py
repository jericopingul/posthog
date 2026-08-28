from dataclasses import dataclass, field
from typing import Any, Literal, Optional

from products.warehouse_sources.backend.types import IncrementalField, IncrementalFieldType

PaginationStyle = Literal["cursor", "page", "offset", "window", "none"]


@dataclass
class DatadogEndpointConfig:
    name: str
    path: str
    # Key in the response body holding the list of records. ``None`` means the body itself is the list.
    data_path: Optional[str] = None
    primary_key: str = "id"
    pagination: PaginationStyle = "none"
    page_size: int = 100
    # Pagination param names (only the ones relevant to ``pagination`` are set per endpoint).
    page_size_param: Optional[str] = None
    page_index_param: Optional[str] = None  # zero-indexed page number
    offset_param: Optional[str] = None  # row offset
    # v2 JSON:API records nest their useful fields under ``attributes``; flatten them to the root.
    flatten_attributes: bool = False
    # Stable, immutable datetime field used for partitioning (never ``modified``/``updated``).
    partition_key: Optional[str] = None
    incremental_fields: list[IncrementalField] = field(default_factory=list)
    default_incremental_field: Optional[str] = None
    # Server-side timestamp filter param (e.g. ``filter[from]``). Only set when the API genuinely
    # filters server-side — leaving it ``None`` keeps the endpoint full-refresh only.
    timestamp_filter_param: Optional[str] = None
    # Value for the ``sort`` query param. For incremental endpoints this must be an ascending,
    # monotonic field so the pipeline's watermark advances correctly.
    sort_param: Optional[str] = None
    # First-sync lookback window for endpoints with a server-side timestamp filter. Datadog's
    # event-search endpoints default ``filter[from]`` to ``now-15m`` when it's omitted, so without
    # this the very first sync would only fetch the last 15 minutes. We seed ``filter[from]`` to
    # ``now - default_lookback_days`` instead; Datadog clamps it to the account's retention.
    # ``window`` endpoints reuse it as the span the window walk covers each sync.
    default_lookback_days: Optional[int] = None
    method: Literal["GET", "POST"] = "GET"
    # JSON:API ``data.type`` for a POST request body (e.g. ``search_request``).
    request_type: Optional[str] = None
    # Attributes merged into every POST request body, alongside the per-request window bounds.
    static_request_attributes: dict[str, Any] = field(default_factory=dict)
    # Value for the ``include`` query param, which asks Datadog to sideload related resources
    # into a top-level ``included`` array.
    include_param: Optional[str] = None
    # Sideloaded resource whose ``attributes`` are merged into each record. Doubles as the
    # ``relationships`` key that names the record's related id, which the search endpoint keeps
    # identical to the ``included`` entry's ``type``.
    included_type: Optional[str] = None
    # Response fields holding epoch-millisecond integers, converted to ISO 8601 on yield.
    epoch_ms_fields: tuple[str, ...] = ()
    # ``window`` pagination: span of the first time slice, and the floor a slice narrows to when
    # it comes back capped at ``page_size``.
    window_initial_hours: int = 24
    window_min_hours: int = 1

    @property
    def supports_incremental(self) -> bool:
        return self.timestamp_filter_param is not None


def _timestamp_incremental_fields(field_name: str) -> list[IncrementalField]:
    return [
        {
            "label": field_name,
            "type": IncrementalFieldType.DateTime,
            "field": field_name,
            "field_type": IncrementalFieldType.DateTime,
        }
    ]


# Endpoint catalog. Coverage mirrors the canonical Datadog streams exposed by the Airbyte and
# Fivetran connectors (logs, audit logs, events, dashboards, monitors, users, incidents, SLOs,
# synthetic tests, downtimes).
#
# Incremental vs full refresh: only the v2 event-style endpoints (logs / audit_logs / events)
# expose a genuine server-side timestamp filter (``filter[from]``) and an ascending ``timestamp``
# sort, so only those are marked incremental. The list/config endpoints have no server-side time
# filter, so they ship as full refresh and dedupe on their primary key.
DATADOG_ENDPOINTS: dict[str, DatadogEndpointConfig] = {
    # --- Append-only, server-side timestamp filter (incremental) ---
    "logs": DatadogEndpointConfig(
        name="logs",
        path="/api/v2/logs/events",
        data_path="data",
        pagination="cursor",
        page_size=1000,
        page_size_param="page[limit]",
        flatten_attributes=True,
        partition_key="timestamp",
        incremental_fields=_timestamp_incremental_fields("timestamp"),
        default_incremental_field="timestamp",
        timestamp_filter_param="filter[from]",
        sort_param="timestamp",
        default_lookback_days=30,
    ),
    "audit_logs": DatadogEndpointConfig(
        name="audit_logs",
        path="/api/v2/audit/events",
        data_path="data",
        pagination="cursor",
        page_size=1000,
        page_size_param="page[limit]",
        flatten_attributes=True,
        partition_key="timestamp",
        incremental_fields=_timestamp_incremental_fields("timestamp"),
        default_incremental_field="timestamp",
        timestamp_filter_param="filter[from]",
        sort_param="timestamp",
        default_lookback_days=30,
    ),
    "events": DatadogEndpointConfig(
        name="events",
        path="/api/v2/events",
        data_path="data",
        pagination="cursor",
        page_size=1000,
        page_size_param="page[limit]",
        flatten_attributes=True,
        partition_key="timestamp",
        incremental_fields=_timestamp_incremental_fields("timestamp"),
        default_incremental_field="timestamp",
        timestamp_filter_param="filter[from]",
        sort_param="timestamp",
        default_lookback_days=30,
    ),
    # --- Window-walked search (full refresh) ---
    "error_tracking_issues": DatadogEndpointConfig(
        name="error_tracking_issues",
        path="/api/v2/error-tracking/issues/search",
        data_path="data",
        method="POST",
        request_type="search_request",
        # ``query: "*"`` matches every issue and ``persona: "ALL"`` spans browser, mobile and
        # backend — the API rejects a search that sets neither ``persona`` nor ``track``. Both are
        # fixed here rather than exposed as connect-form fields, so the synced table stays useful
        # to every consumer instead of being locked to one persona.
        static_request_attributes={"query": "*", "persona": "ALL", "order_by": "FIRST_SEEN"},
        # The request schema has no limit/page/offset and the response carries neither ``meta`` nor
        # ``links``; Datadog caps a search at 100 issues. Paging therefore walks the ``from``/``to``
        # window instead of a server cursor.
        pagination="window",
        page_size=100,
        include_param="issue",
        included_type="issue",
        flatten_attributes=True,
        epoch_ms_fields=("first_seen", "last_seen"),
        partition_key="first_seen",
        # Full refresh, not incremental: ``from``/``to`` bound when an issue's *errors* happened,
        # not when the issue was first seen. A long-standing issue that errors today comes back
        # with a years-old ``first_seen``, so no server-side filter on ``first_seen`` exists and
        # rows do not arrive ordered by it. Each sync re-walks the lookback window and merges on
        # the issue id, which is also what keeps state and counts current as issues are triaged.
        default_lookback_days=30,
    ),
    # --- Full refresh ---
    "dashboards": DatadogEndpointConfig(
        name="dashboards",
        path="/api/v1/dashboard",
        data_path="dashboards",
        pagination="none",
        partition_key="created_at",
    ),
    "monitors": DatadogEndpointConfig(
        name="monitors",
        path="/api/v1/monitor",
        data_path=None,
        pagination="page",
        page_size=100,
        page_size_param="page_size",
        page_index_param="page",
        partition_key="created",
    ),
    "users": DatadogEndpointConfig(
        name="users",
        path="/api/v2/users",
        data_path="data",
        pagination="page",
        page_size=100,
        page_size_param="page[size]",
        page_index_param="page[number]",
        flatten_attributes=True,
        partition_key="created_at",
    ),
    "incidents": DatadogEndpointConfig(
        name="incidents",
        path="/api/v2/incidents",
        data_path="data",
        pagination="offset",
        page_size=100,
        page_size_param="page[size]",
        offset_param="page[offset]",
        flatten_attributes=True,
        partition_key="created",
    ),
    "slos": DatadogEndpointConfig(
        name="slos",
        path="/api/v1/slo",
        data_path="data",
        pagination="offset",
        page_size=100,
        page_size_param="limit",
        offset_param="offset",
        # SLO ``created_at`` is a unix epoch integer rather than an ISO datetime, so it isn't a
        # safe partition key — left unpartitioned.
    ),
    "synthetic_tests": DatadogEndpointConfig(
        name="synthetic_tests",
        path="/api/v1/synthetics/tests",
        data_path="tests",
        pagination="none",
        primary_key="public_id",
    ),
    "downtimes": DatadogEndpointConfig(
        name="downtimes",
        path="/api/v2/downtime",
        data_path="data",
        pagination="offset",
        page_size=100,
        page_size_param="page[limit]",
        offset_param="page[offset]",
        flatten_attributes=True,
    ),
}

# Vendor API versions. Datadog serves each resource under a fixed API version, so this source
# already reads logs/audit_logs/events/users/incidents/downtimes at /api/v2 and
# dashboards/monitors/slos/synthetic_tests at /api/v1 (those four have no v2 list endpoint). Both
# labels therefore resolve to the identical request paths; the pin only selects the version a new
# source is stamped with and drives the deprecation warning for v1.
DATADOG_API_VERSION_V1 = "v1"
DATADOG_API_VERSION_V2 = "v2"
DATADOG_SUPPORTED_VERSIONS = (DATADOG_API_VERSION_V1, DATADOG_API_VERSION_V2)
DATADOG_DEFAULT_VERSION = DATADOG_API_VERSION_V2

ENDPOINTS = tuple(DATADOG_ENDPOINTS.keys())

INCREMENTAL_FIELDS: dict[str, list[IncrementalField]] = {
    name: config.incremental_fields for name, config in DATADOG_ENDPOINTS.items()
}

# Datadog retains logs / audit logs / events for a limited window, so the first sync can only
# reach back as far as the account's retention allows.
LIMITED_RETENTION_ENDPOINTS = {"logs", "audit_logs", "events"}
