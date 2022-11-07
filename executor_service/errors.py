class APIError(Exception):
    status_code: int = None  # type: ignore
    message: str = None  # type: ignore

    def __init__(self, message: str = None):
        super().__init__(message)
        if message:
            self.message = message


class QueryNotFoundError(APIError):
    status_code = 422

    def __init__(self, query_pid: int):
        super().__init__(f"Query with pid {query_pid} does not exist.")
