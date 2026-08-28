"""Signal emitter for datadog `error_tracking_issues` (record kind: issue).

`first_seen` is an ISO string — the warehouse source converts Datadog's epoch milliseconds on
yield. No link is emitted: `IssueAttributes` carries no permalink, and the synced row does not
record which Datadog site the source reads, so there is no way to build an absolute URL. The
issue id rides on `source_id`, which is what a later lookup would need.
"""

from products.signals.backend.emission._common import make_flat_emitter
from products.signals.backend.emission._prompts import ERROR_ACTIONABILITY_PROMPT, ERROR_SUMMARIZATION_PROMPT
from products.signals.backend.emission.fetchers.data_warehouse import data_warehouse_record_fetcher
from products.signals.backend.emission.registry import SignalSourceTableConfig

DATADOG_ERROR_TRACKING_FIELDS = (
    "id",
    "error_type",
    "error_message",
    "service",
    "state",
    "is_crash",
    "platform",
    "languages",
    "file_path",
    "function_name",
    "impacted_users",
    "total_count",
    "first_seen",
)

DATADOG_ERROR_TRACKING_CONFIG = SignalSourceTableConfig(
    source_product="datadog",
    source_type="issue",
    emitter=make_flat_emitter(
        source_product="datadog",
        source_type="issue",
        id_field="id",
        title_field="error_type",
        body_field="error_message",
        extra_fields=(
            "service",
            "state",
            "is_crash",
            "platform",
            "languages",
            "file_path",
            "function_name",
            "impacted_users",
            "total_count",
            "first_seen",
        ),
        json_list_fields=("languages",),
    ),
    record_fetcher=data_warehouse_record_fetcher,
    partition_field="first_seen",
    partition_field_is_datetime_string=True,
    fields=DATADOG_ERROR_TRACKING_FIELDS,
    # Narrowing to untriaged backend errors belongs here, not in the warehouse endpoint config:
    # `states` is a server-side filter on the search request, but applying it there would bake one
    # consumer's filter into a table every consumer shares.
    where_clause="state IN ('OPEN', 'ACKNOWLEDGED') AND platform = 'BACKEND'",
    max_records=500,
    first_sync_lookback_days=1,
    actionability_prompt=ERROR_ACTIONABILITY_PROMPT,
    summarization_prompt=ERROR_SUMMARIZATION_PROMPT,
    description_summarization_threshold_chars=2000,
)
