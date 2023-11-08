import pytest


@pytest.mark.anyio()
async def test_default() -> None:  # noqa: D103
    assert str(1) == "1"  # noqa: S101
