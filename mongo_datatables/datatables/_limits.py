"""Define resource limits for aggregation pipeline complexity and pagination."""

# Maximum number of caller-supplied pre-match pipeline stages.
MAX_PIPELINE_STAGES: int = 20

# Maximum number of $facet branches in a single SearchPanes aggregation.
MAX_FACET_BRANCHES: int = 50

# Maximum number of options returned per SearchPanes column.
MAX_PANE_OPTIONS: int = 1000

# Maximum page size a client may request via the `length` parameter.
MAX_PAGE_SIZE: int = 1000

# Default page size when the client supplies an invalid or negative value.
DEFAULT_PAGE_SIZE: int = 10
