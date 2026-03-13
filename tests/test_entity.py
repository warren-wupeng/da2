from da2 import Entity, Event


class UserCreated(Event):
    def __init__(self, user_id: str):
        self.user_id = user_id


class User(Entity[str, dict]):
    def rename(self, new_name: str):
        self._desc["name"] = new_name
        self.raise_event(UserCreated(self.identity))


class TestEntity:

    def test_create_entity(self):
        user = User(identity="u1", desc={"name": "Alice"})
        assert user.identity == "u1"
        assert user.desc["name"] == "Alice"

    def test_repr(self):
        user = User(identity="u1", desc={})
        assert "User(identity=u1)" == repr(user)

    def test_raise_event(self):
        user = User(identity="u1", desc={"name": "Alice"})
        assert len(user.events) == 0

        user.rename("Bob")
        assert user.desc["name"] == "Bob"
        assert len(user.events) == 1
        assert isinstance(user.events[0], UserCreated)
        assert user.events[0].user_id == "u1"

    def test_timestamps(self):
        user = User(identity="u1", desc={}, created_at=100, updated_at=200)
        assert user.created_at == 100
        assert user.updated_at == 200

    def test_not_found(self):
        try:
            raise Entity.NotFound("not found")
        except Entity.NotFound as e:
            assert str(e) == "not found"
