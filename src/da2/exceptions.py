class EntityNotFound(Exception):
    """Raised when an entity cannot be found by its identity."""

    pass


class ConcurrencyError(Exception):
    """Raised when expected_version doesn't match the stored version.

    This indicates a write conflict -- another process appended events
    to the same aggregate between your read and write.
    """

    pass
