import typing


class APIError(Exception):
    def __init__(self, message: str = None):
        super().__init__(message)
        if message:
            self.message = message


class QueryError(APIError):
    message: str = 'Error'

    def __init__(self, *, query_id: typing.Optional[int] = None, query_guid: typing.Optional[int] = None):
        self.query_id = query_id
        self.query_guid = query_guid
        super().__init__(f'Query {query_id or query_guid}. {self.__class__.message}')


class QueryNotFoundError(QueryError):
    message: str = 'Query does not exist'


class QueryNotRunning(QueryError):
    message: str = 'Query is not in the running state'
