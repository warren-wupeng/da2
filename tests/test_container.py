import pytest
from da2 import Container


class TestContainer:

    def test_register_and_resolve_value(self):
        c = Container()
        c.register("db_url", "sqlite:///test.db")
        assert c.resolve("db_url") == "sqlite:///test.db"

    def test_register_and_resolve_factory(self):
        c = Container()
        c.register("greeting", lambda: "hello")
        assert c.resolve("greeting") == "hello"

    def test_resolve_missing_raises(self):
        c = Container()
        with pytest.raises(KeyError):
            c.resolve("missing")

    def test_separate_instances(self):
        c1 = Container()
        c2 = Container()
        c1.register("x", 1)
        c2.register("x", 2)
        assert c1.resolve("x") == 1
        assert c2.resolve("x") == 2
