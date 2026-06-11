from actorium import create_actor_system_and_run, spawn
from actorium.actors.registry import Registration, Registry


def test_registry() -> None:
    async def main() -> None:
        # Create a registry.
        registry = spawn(Registry[int])

        # Store a value.
        spawn(Registration[int], registry, "name1", 1)

        # Retrieve a value.
        result = await registry.get("name1", 1)
        assert result == 1

        # List keys.
        keys = await registry.keys()
        assert keys == ["name1"]

    create_actor_system_and_run(main)
