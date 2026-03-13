from da2 import Entity, Command, Event, UnitOfWork, MessageBus


class OrderPlaced(Event):
    def __init__(self, order_id: str):
        self.order_id = order_id


class SendConfirmation(Event):
    def __init__(self, order_id: str):
        self.order_id = order_id


class PlaceOrder(Command):
    def __init__(self, order_id: str, product: str):
        self.order_id = order_id
        self.product = product


class Order(Entity[str, dict]):
    pass


class FakeUnitOfWork(UnitOfWork):

    def __init__(self):
        self.committed = False

    def _enter(self):
        pass

    def _commit(self):
        self.committed = True

    def rollback(self):
        pass


class TestMessageBus:

    def test_handle_command(self):
        results = []
        uow = FakeUnitOfWork()
        uow.seen = {}

        def handle_place_order(cmd: PlaceOrder):
            results.append(f"order:{cmd.order_id}")

        bus = MessageBus(
            uow=uow,
            event_handlers={},
            command_handlers={PlaceOrder: handle_place_order},
        )
        bus.handle(PlaceOrder(order_id="o1", product="widget"))
        assert results == ["order:o1"]

    def test_handle_event(self):
        results = []
        uow = FakeUnitOfWork()
        uow.seen = {}

        def on_order_placed(event: OrderPlaced):
            results.append(f"confirmed:{event.order_id}")

        bus = MessageBus(
            uow=uow,
            event_handlers={OrderPlaced: [on_order_placed]},
            command_handlers={},
        )
        bus.handle(OrderPlaced(order_id="o1"))
        assert results == ["confirmed:o1"]

    def test_command_triggers_event_chain(self):
        results = []
        uow = FakeUnitOfWork()
        uow.seen = {}

        order = Order(identity="o1", desc={"product": "widget"})
        uow.add_seen(order)

        def handle_place_order(cmd: PlaceOrder):
            order.raise_event(OrderPlaced(order_id=cmd.order_id))

        def on_order_placed(event: OrderPlaced):
            results.append(f"confirmed:{event.order_id}")

        bus = MessageBus(
            uow=uow,
            event_handlers={OrderPlaced: [on_order_placed]},
            command_handlers={PlaceOrder: handle_place_order},
        )
        bus.handle(PlaceOrder(order_id="o1", product="widget"))
        assert results == ["confirmed:o1"]

    def test_event_handler_exception_does_not_stop_others(self):
        results = []
        uow = FakeUnitOfWork()
        uow.seen = {}

        def bad_handler(event):
            raise ValueError("boom")

        def good_handler(event):
            results.append("ok")

        bus = MessageBus(
            uow=uow,
            event_handlers={OrderPlaced: [bad_handler, good_handler]},
            command_handlers={},
        )
        bus.handle(OrderPlaced(order_id="o1"))
        assert results == ["ok"]
