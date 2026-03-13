import pytest
from da2 import Entity, Event, UnitOfWork, UnitOfWorkAsync


class ItemAdded(Event):
    def __init__(self, item_id: str):
        self.item_id = item_id


class Cart(Entity[str, dict]):
    def add_item(self, item_id: str):
        self.raise_event(ItemAdded(item_id=item_id))


class FakeUoW(UnitOfWork):
    def __init__(self):
        self.committed = False
        self.rolled_back = False

    def _enter(self):
        pass

    def _commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class FakeUoWAsync(UnitOfWorkAsync):
    def __init__(self):
        self.committed = False
        self.rolled_back = False

    async def _enter(self):
        pass

    async def _commit(self):
        self.committed = True

    async def rollback(self):
        self.rolled_back = True

    async def _exit(self, *args):
        pass


class TestUnitOfWork:

    def test_context_manager(self):
        uow = FakeUoW()
        with uow:
            assert uow.seen == {}
            uow.commit()
        assert uow.committed

    def test_rollback_on_exception(self):
        uow = FakeUoW()
        try:
            with uow:
                raise ValueError("boom")
        except ValueError:
            pass
        assert uow.rolled_back

    def test_add_seen_and_collect_events(self):
        uow = FakeUoW()
        with uow:
            cart = Cart(identity="c1", desc={})
            uow.add_seen(cart)
            cart.add_item("item1")
            cart.add_item("item2")

            events = list(uow.collect_new_events())
            assert len(events) == 2
            assert events[0].item_id == "item1"
            assert events[1].item_id == "item2"
            assert len(cart.events) == 0


class TestUnitOfWorkAsync:

    @pytest.mark.asyncio
    async def test_context_manager(self):
        uow = FakeUoWAsync()
        async with uow:
            assert uow.seen == {}
            await uow.commit()
        assert uow.committed

    @pytest.mark.asyncio
    async def test_rollback_on_exception(self):
        uow = FakeUoWAsync()
        try:
            async with uow:
                raise ValueError("boom")
        except ValueError:
            pass
        assert uow.rolled_back
