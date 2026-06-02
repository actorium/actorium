from actorium import Mailbox, SimpleActor, run
from actorium.actors.registry import Registration, Registry
from actorium.system import spawn


def test_registry() -> None:
    class Main(SimpleActor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            # Create a registry.
            registry = spawn(Registry[int])

            # Store a value.
            spawn(Registration[int], registry, "name1", 1)

            # Retrieve a value.
            result = await registry.rpc.get("name1", 1)
            assert result == 1

            # List keys.
            keys = await registry.rpc.keys()
            assert keys == ["name1"]

    run(Main)
