"""The default SQL client's typed failures."""


class SqlClientError(Exception):
    """Base of the typed client failures."""


class DatabaseUnavailable(SqlClientError):
    """The database could not be reached."""


class AuthenticationRefused(SqlClientError):
    """The database rejected the client's credentials."""


class ObjectNotFound(SqlClientError):
    """A metadata read named a schema or table that does not exist."""


class SessionStatementRefused(SqlClientError):
    """``execute`` was given a statement that changes session state or controls transactions."""


class WriteStatementRefused(SqlClientError):
    """``execute`` or ``explain`` was given a statement it cannot classify as a read."""
