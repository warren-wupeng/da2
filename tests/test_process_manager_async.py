"""Tests for ProcessManagerAsync."""

import pytest

from da2 import Command, Event
from da2.process_manager_async import ProcessManagerAsync


# ── Fixtures ──────────────────────────────────────────────────────

class DoSomething(Command):
    def __init__(self, value: str):
        self.value = value


class DoAnother(Command):
    def __init__(self, value: str):
        self.value = value


class ThingHappened(Event):
    def __init__(self, value: str):
        self.value = value


class StepCompleted(Event):
    def __init__(self, value: str):
        self.value = value


class AsyncWorkflow(ProcessManagerAsync):
    def __init__(self, process_id: str):
        super().__init__(process_id)
        self.log: list[str] = []

    async def _on_ThingHappened(self, event: ThingHappened):
        self.log.append(f"async:{event.value}")
        self._dispatch(DoSomething(value=event.value))

    def _on_StepCompleted(self, event: StepCompleted):
        self.log.append(f"sync:{event.value}")
        self._dispatch(DoAnother(value=event.value))
        self._mark_completed()


# ── Tests ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_handle():
    pm = AsyncWorkflow("p-1")
    cmds = await pm.handle(ThingHappened(value="go"))
    assert len(cmds) == 1
    assert isinstance(cmds[0], DoSomething)
    assert cmds[0].value == "go"
    assert pm.log == ["async:go"]


@pytest.mark.asyncio
async def test_sync_handler_in_async_pm():
    pm = AsyncWorkflow("p-1")
    cmds = await pm.handle(StepCompleted(value="done"))
    assert len(cmds) == 1
    assert isinstance(cmds[0], DoAnother)
    assert pm.completed
    assert pm.log == ["sync:done"]


@pytest.mark.asyncio
async def test_async_full_lifecycle():
    pm = AsyncWorkflow("p-1")
    assert not pm.completed

    cmds1 = await pm.handle(ThingHappened(value="start"))
    assert not pm.completed
    assert len(cmds1) == 1

    cmds2 = await pm.handle(StepCompleted(value="finish"))
    assert pm.completed
    assert len(cmds2) == 1
    assert pm.log == ["async:start", "sync:finish"]


@pytest.mark.asyncio
async def test_async_unknown_event():
    class Random(Event):
        pass

    pm = AsyncWorkflow("p-1")
    cmds = await pm.handle(Random())
    assert cmds == []
    assert pm.log == []
