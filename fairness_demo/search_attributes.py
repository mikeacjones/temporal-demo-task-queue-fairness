"""Search Attributes the dashboard reads through Visibility.

Register them once per Namespace: ``scripts/dev-server.sh`` does it locally; on Temporal
Cloud, add them in the Namespace settings.
"""

from temporalio.common import SearchAttributeKey

# "<task queue>-<browser session>-<run>": scopes every listing and reset to one run.
DOC_SESSION = SearchAttributeKey.for_keyword("DocSession")
DOC_TENANT = SearchAttributeKey.for_keyword("DocTenant")
# When the document's first Activity started on a Worker: the end of its wait.
DOC_STARTED_AT = SearchAttributeKey.for_datetime("DocStartedAt")
