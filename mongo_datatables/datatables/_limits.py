"""Resource limits for aggregation pipeline complexity."""

# Maximum number of caller-supplied pre-match pipeline stages.
MAX_PIPELINE_STAGES: int = 20

# Maximum number of $facet branches in a single SearchPanes aggregation.
MAX_FACET_BRANCHES: int = 50

# Maximum number of options returned per SearchPanes column.
MAX_PANE_OPTIONS: int = 1000
