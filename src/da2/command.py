class Command:
    """Base class for commands — a write intent from the outside world.

    Subclass this to define commands handled by the MessageBus.
    Each command type has exactly one handler.

    Example::

        class CreateOrder(Command):
            def __init__(self, customer_id: str, product: str):
                self.customer_id = customer_id
                self.product = product

        cmd = CreateOrder(customer_id="c-1", product="Widget")
        assert cmd.customer_id == "c-1"
    """

    pass
