"""Event upcasting for schema evolution in event-sourced systems.

When event schemas change over time (e.g. a field is added, renamed, or
an event type is split), stored events in the old format need to be
transformed to the current schema during loading.  Upcasters perform
this transformation.

Example::

    from da2.upcaster import Upcaster, UpcasterChain

    class AddCurrencyToOrderPlaced(Upcaster):
        event_type = "OrderPlaced"

        def upcast(self, data: dict) -> dict:
            data.setdefault("currency", "USD")
            return data

    chain = UpcasterChain([AddCurrencyToOrderPlaced()])
    event_type, data = chain.upcast("OrderPlaced", {"total": 99.0})
    assert data == {"total": 99.0, "currency": "USD"}
"""

from __future__ import annotations

import abc


class Upcaster(abc.ABC):
    """Transform stored event data from an older schema to a newer one.

    Subclass and set ``event_type`` to the stored event type name this
    upcaster handles.  Implement ``upcast()`` to transform the data dict.

    Optionally override ``target_event_type`` to rename the event type
    (e.g. when ``OrderPlaced`` is renamed to ``OrderCreated``).

    Example::

        class RenameAmountToTotal(Upcaster):
            event_type = "OrderPlaced"

            def upcast(self, data: dict) -> dict:
                if "amount" in data:
                    data["total"] = data.pop("amount")
                return data

        class RenameOrderPlaced(Upcaster):
            event_type = "OrderPlaced"
            target_event_type = "OrderCreated"

            def upcast(self, data: dict) -> dict:
                return data
    """

    event_type: str
    """The stored event type name this upcaster matches against."""

    target_event_type: str | None = None
    """If set, renames the event type after upcasting."""

    @abc.abstractmethod
    def upcast(self, data: dict) -> dict:
        """Transform the event data dict. Return the updated dict."""
        raise NotImplementedError


class UpcasterChain:
    """Apply a sequence of upcasters to stored events during loading.

    Upcasters are applied in registration order.  If an upcaster renames
    the event type (via ``target_event_type``), subsequent upcasters
    match against the new name.

    Example::

        chain = UpcasterChain([
            AddCurrencyField(),      # OrderPlaced -> adds "currency"
            RenameToOrderCreated(),  # OrderPlaced -> OrderCreated
        ])
        event_type, data = chain.upcast("OrderPlaced", {"total": 99.0})
        assert event_type == "OrderCreated"
        assert data["currency"] == "USD"
    """

    def __init__(self, upcasters: list[Upcaster] | None = None) -> None:
        self._upcasters: list[Upcaster] = list(upcasters or [])

    def add(self, upcaster: Upcaster) -> None:
        """Append an upcaster to the chain."""
        self._upcasters.append(upcaster)

    def upcast(self, event_type: str, data: dict) -> tuple[str, dict]:
        """Apply all matching upcasters in order.

        Returns ``(final_event_type, final_data)``.  The input ``data``
        dict is **not** mutated; a copy is made before transformations.
        """
        current_type = event_type
        current_data = dict(data)
        for u in self._upcasters:
            if u.event_type == current_type:
                current_data = u.upcast(current_data)
                if u.target_event_type is not None:
                    current_type = u.target_event_type
        return current_type, current_data

    def __len__(self) -> int:
        return len(self._upcasters)

    def __repr__(self) -> str:
        return f"UpcasterChain(upcasters={self._upcasters!r})"
