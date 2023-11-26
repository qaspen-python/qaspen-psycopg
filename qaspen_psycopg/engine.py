from __future__ import annotations

import contextvars
import types
import warnings
from typing import TYPE_CHECKING, Any, Final, Literal, overload

from psycopg import AsyncConnection, AsyncCursor
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from qaspen.abc.db_engine import BaseEngine
from qaspen.abc.db_transaction import BaseTransaction

if TYPE_CHECKING:
    from typing_extensions import Self


class PsycopgTransaction(
    BaseTransaction[
        "PsycopgEngine",
        AsyncConnection[Any],
    ],
):
    """Transaction for `PsycopgEngine`.

    It allows us to use async context manager.

    Example:
    -------
    ```python
    class Buns(BaseTable, table_name="buns"):
        name: VarCharField = VarCharField()


    engine = PsycopgEngine(
        connection_url="postgres://postgres:postgres@localhost:5432/qaspen",
    )

    async def main() -> None:
        await engine.create_connection_pool()

        async with engine.transaction():
            await Buns.select()
            await Buns.select()
            # At the end of this block this select queries will be commit.
    ```
    """

    engine_type: str = "PSQLPsycopg"

    def __init__(
        self: Self,
        engine: PsycopgEngine,
    ) -> None:
        super().__init__(engine=engine)
        self._connection: AsyncConnection[Any] | None = None
        self._transaction: AsyncCursor[Any] | None = None

        self._is_rollback_executed: bool = False
        self._is_commit_executed: bool = False

    async def __aenter__(self: Self) -> Self:
        """Enter in the async context manager.

        This method must setup new transaction.

        ### Returns:
        New transaction context manager.
        """
        self._connection = await self.retrieve_connection()
        self._transaction = _retrieve_cursor(
            connection=self._connection,
        )
        self.context = self.engine.running_transaction.set(self)
        return self

    async def __aexit__(
        self: Self,
        _exception_type: type[BaseException] | None,
        exception: BaseException | None,
        _traceback: types.TracebackType | None,
    ) -> None:
        """Exit the async context manager.

        If there is an exception, rollback the transaction.
        If there is no exception, user haven't commit or rolledback
        the transaction, commit the transaction.

        Then return connection to the connection pool.
        """
        rollback_condition = exception and not self._is_rollback_executed
        commit_condition = (
            not exception
            and not self._is_commit_executed
            and not self._is_rollback_executed
        )
        if rollback_condition:
            await self.rollback()
        elif commit_condition:
            await self.commit()

        conn_pool = await self.engine.connection_pool
        await conn_pool.putconn(self._connection)

        self.engine.running_transaction.reset(self.context)

    @overload
    async def execute(  # type: ignore[misc]
        self: Self,
        querystring: str,
        fetch_results: Literal[True] = True,
    ) -> list[dict[str, Any]]:
        ...

    @overload
    async def execute(
        self: Self,
        querystring: str,
        fetch_results: Literal[False] = False,
    ) -> None:
        ...

    @overload
    async def execute(
        self: Self,
        querystring: str,
        fetch_results: bool,
    ) -> list[dict[str, Any]] | None:
        ...

    async def execute(
        self: Self,
        querystring: str,
        fetch_results: bool = True,
    ) -> list[dict[str, Any]] | None:
        """Execute querystring.

        ### Parameters:
        - `querystring`: sql querystring to execute.
        - `fetch_results`: Get results or not,
            Possible only for queries that return something.
        """
        results: list[dict[str, Any]] | None = None

        if not self._connection:
            self._connection = await self.retrieve_connection()
            self._transaction = _retrieve_cursor(
                connection=self._connection,
            )

        result_cursor: Final = (
            await self._transaction.execute(  # type: ignore[union-attr]
                query=querystring,
            )
        )

        if fetch_results:
            results = await result_cursor.fetchall()

        return results

    async def retrieve_connection(self: Self) -> AsyncConnection[Any]:
        """Retrieve new connection from the engine.

        ### Returns:
        `AsyncConnection`.
        """
        connection_pool = await self.engine.connection_pool
        return await connection_pool.getconn()

    async def rollback(self: Self) -> None:
        """Rollback the transaction.

        And set `_is_rollback_executed` flag to True.
        """
        await self._connection.rollback()  # type: ignore[union-attr]
        self._is_rollback_executed = True

    async def commit(self: Self) -> None:
        """Commit the transaction.

        And set `_is_commit_executed` flag to True.
        """
        await self._connection.commit()  # type: ignore[union-attr]
        self._is_commit_executed = True

    async def begin(self: Self) -> None:
        return None


class PsycopgEngine(
    BaseEngine[
        AsyncConnection[Any],
        AsyncConnectionPool,
        PsycopgTransaction,
    ],
):
    """Engine for PostgreSQL based on `psycopg`."""

    engine_type: str = "PSQLPsycopg"

    def __init__(
        self: Self,
        connection_url: str,
        open_connection_pool_wait: bool | None = None,
        open_connection_pool_timeout: float | None = None,
        close_connection_pool_timeout: float | None = None,
        connection_pool_params: dict[str, Any] | None = None,
    ) -> None:
        self.connection_url = connection_url
        self.running_transaction: contextvars.ContextVar[
            PsycopgTransaction | None,
        ] = contextvars.ContextVar(
            "running_transaction",
            default=None,
        )
        self.connection_pool_params = connection_pool_params or {}
        self.open_connection_pool_wait = open_connection_pool_wait
        self.open_connection_pool_timeout = open_connection_pool_timeout
        self.close_connection_pool_timeout = close_connection_pool_timeout

        self._connection_pool: AsyncConnectionPool | None = None

    @property
    async def connection_pool(
        self: Self,
    ) -> AsyncConnectionPool:
        """Property for connection pool."""
        if not self._connection_pool:
            return await self.create_connection_pool()
        return self._connection_pool

    @overload
    async def execute(  # type: ignore[misc]
        self: Self,
        querystring: str,
        in_pool: bool = True,
        fetch_results: Literal[True] = True,
        **_kwargs: Any,
    ) -> list[dict[str, Any]]:
        ...

    @overload
    async def execute(
        self: Self,
        querystring: str,
        in_pool: bool = True,
        fetch_results: Literal[False] = False,
        **_kwargs: Any,
    ) -> None:
        ...

    @overload
    async def execute(
        self: Self,
        querystring: str,
        in_pool: bool = True,
        fetch_results: bool = True,
        **_kwargs: Any,
    ) -> list[dict[str, Any]] | None:
        ...

    async def execute(
        self: Self,
        querystring: str,
        in_pool: bool = True,
        fetch_results: bool = True,
        **_kwargs: Any,
    ) -> list[dict[str, Any]] | None:
        """Execute a querystring.

        Run querystring and return raw result as in
        database driver.

        ### Parameters:
        - `querystring`: `QueryString` or it's subclasses.
        - `in_pool`: execution in connection pool
            or in a new connection.
        - `fetch_results`: Get results or not,
            Possible only for queries that return something.
        - `kwargs`: just for inheritance, subclasses won't
            have problems with type hints.

        ### Returns:
        Raw result from database driver.
        """
        results: list[dict[str, Any]] | None = None
        if running_transaction := self.running_transaction.get():
            results = await running_transaction.execute(
                querystring=querystring,
                fetch_results=fetch_results,
            )

        elif in_pool:
            conn_pool: Final = await self.connection_pool
            connection = await conn_pool.getconn()
            cursor = _retrieve_cursor(
                connection=connection,
            )
            cursor = await cursor.execute(
                querystring,
            )
            if fetch_results:
                results = await cursor.fetchall()

            await connection.commit()
            await conn_pool.putconn(connection)

        else:
            connection = await self.connection()
            cursor = _retrieve_cursor(
                connection,
            )
            cursor = await cursor.execute(
                querystring,
            )
            if fetch_results:
                results = await cursor.fetchall()

            await connection.commit()

        return results

    async def prepare_database(self: Self) -> None:
        """Prepare database.

        Create necessary extensions.
        """

    async def create_connection_pool(self: Self) -> AsyncConnectionPool:
        """Create new connection pool.

        If connection pool already exists return it.

        ### Returns:
        `AsyncConnectionPool`
        """
        if not self._connection_pool:
            self._connection_pool = AsyncConnectionPool(
                conninfo=self.connection_url,
                **self.connection_pool_params,
            )
            await self._connection_pool.open(
                wait=self.open_connection_pool_wait or True,
                timeout=self.open_connection_pool_timeout or 30,
            )

        return self._connection_pool

    async def stop_connection_pool(
        self: Self,
    ) -> None:
        """Close connection pool.

        If connection pool doesn't exist, raise an error.
        """
        if not self._connection_pool:
            warnings.warn(
                "Try to close not existing connection pool.",
                stacklevel=2,
            )
            return

        await self._connection_pool.close(
            timeout=self.close_connection_pool_timeout or 30,
        )

    async def connection(self: Self) -> AsyncConnection[Any]:
        """Create new connection outside connection pool.

        ### Returns:
        initialized `AsyncConnection`.
        """
        return await AsyncConnection.connect(
            conninfo=self.connection_url,
        )

    def transaction(self: Self) -> PsycopgTransaction:
        """Create new transaction.

        ### Returns:
        New `PsycopgTransaction`.
        """
        return PsycopgTransaction(engine=self)


def _retrieve_cursor(
    connection: AsyncConnection,
) -> AsyncCursor:
    """Create cursor for the connection.

    ### Parameters:
    - `connection`: connection to the database.

    ### Returns:
    New `AsyncCursor`
    """
    return connection.cursor(row_factory=dict_row)
