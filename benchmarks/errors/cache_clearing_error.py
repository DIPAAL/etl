"""Module containing custom cache clearing error."""


class CacheClearingError(RuntimeError):
    """Custom cache clearing error."""

    def __init__(self, exit_code: int, message: str) -> None:
        """
        Initialize CacheClearingError.

        Arguments:
            exit_code: the exit code received while attempting to clear cache
            message: the exception message
        """
        self.exit_code = exit_code
        self.message = message
        super().__init__(self.message)
