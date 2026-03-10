"""Custom errors for pipeline orchestration."""


class PipelineError(Exception):
    """Base pipeline error."""


class ConfigError(PipelineError):
    """Configuration/argument validation error."""


class ProviderError(PipelineError):
    """Provider-level failure."""


class StageError(PipelineError):
    """A processing stage failed."""
