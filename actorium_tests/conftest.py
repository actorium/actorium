import pytest


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(scope="session", autouse=True)
def anyio_backend_autouse(anyio_backend: str) -> str:
    return anyio_backend
