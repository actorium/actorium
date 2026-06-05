from actorium import SimpleActor, run
from actorium.actors.registry import Registration, Registry
from actorium.system import spawn


def test_registry() -> None:
    class Main(SimpleActor[None]):
        async def actor_run(self) -> None:
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

    run(Main)
