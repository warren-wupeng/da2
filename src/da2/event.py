class Event:
    """Base class for domain events -- something that happened.

    Subclass this to define events raised by entities. Events are
    collected by the UnitOfWork and dispatched through the MessageBus.

    Events support serialization via ``to_dict()`` / ``from_dict()``
    for use with EventStore (event sourcing).

    Example::

        class OrderPlaced(Event):
            def __init__(self, order_id: str, total: float):
                self.order_id = order_id
                self.total = total

        event = OrderPlaced(order_id="o-1", total=99.0)
        assert event.order_id == "o-1"
        assert event.to_dict() == {"order_id": "o-1", "total": 99.0}

        restored = OrderPlaced.from_dict({"order_id": "o-1", "total": 99.0})
        assert restored.order_id == "o-1"
    """

    def to_dict(self) -> dict:
        """Serialize event to a dict. Default: ``vars(self)``."""
        return dict(vars(self))

    @classmethod
    def from_dict(cls, data: dict) -> "Event":
        """Deserialize event from a dict. Default: ``cls(**data)``."""
        return cls(**data)
