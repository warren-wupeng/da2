import pytest
from da2 import Entity, InMemoryRepository


class User(Entity[str, dict]):
    pass


class UserRepo(InMemoryRepository[User]):
    pass


class TestInMemoryRepository:

    def test_add_and_get(self):
        repo = UserRepo()
        user = User(identity="u1", desc={"name": "Alice"})
        repo.add(user)
        found = repo.get("u1")
        assert found.identity == "u1"
        assert found.desc["name"] == "Alice"

    def test_get_not_found(self):
        repo = UserRepo()
        with pytest.raises(Entity.NotFound):
            repo.get("missing")

    def test_update(self):
        repo = UserRepo()
        user = User(identity="u1", desc={"name": "Alice"})
        repo.add(user)

        updated = User(identity="u1", desc={"name": "Bob"})
        repo.update(updated)

        found = repo.get("u1")
        assert found.desc["name"] == "Bob"

    def test_delete(self):
        repo = UserRepo()
        user = User(identity="u1", desc={"name": "Alice"})
        repo.add(user)
        repo.delete("u1")
        with pytest.raises(Entity.NotFound):
            repo.get("u1")

    def test_find(self):
        repo = UserRepo()
        repo.add(User(identity="u1", desc={"role": "admin"}))
        repo.add(User(identity="u2", desc={"role": "user"}))
        repo.add(User(identity="u3", desc={"role": "admin"}))

        admins = repo.find(lambda u: u.desc["role"] == "admin")
        assert len(admins) == 2

    def test_find_all(self):
        repo = UserRepo()
        repo.add(User(identity="u1", desc={}))
        repo.add(User(identity="u2", desc={}))
        assert len(repo.find()) == 2

    def test_len(self):
        repo = UserRepo()
        assert len(repo) == 0
        repo.add(User(identity="u1", desc={}))
        assert len(repo) == 1

    def test_seen_tracking(self):
        seen = {}
        repo = UserRepo(add_seen=lambda e: seen.update({e.identity: e}))
        user = User(identity="u1", desc={})
        repo.add(user)
        assert "u1" in seen

        fetched = repo.get("u1")
        assert seen["u1"] is fetched
