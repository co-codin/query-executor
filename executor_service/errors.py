class APIError(Exception):
    status_code: int = None  # type: ignore
    message: str = None  # type: ignore

    def __init__(self, message: str = None):
        super().__init__(message)
        if message:
            self.message = message
