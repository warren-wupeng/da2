class Event:
    """Base class for domain events -- something that happened.

    Subclass this to define events raised by entities. Events are
    collected by the UnitOfWork and dispatched through the MessageBus.

    Example::

        class OrderPlaced(Event):
            def __init__(self, order_id: str, total: float):
                self.order_id = order_id
                self.total = total

        event = OrderPlaced(order_id="o-1", total=99.0)
        assert event.order_id == "o-1"
    """

    pass
