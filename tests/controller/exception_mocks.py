from unittest.mock import MagicMock


class PySNAPIBaseException(Exception):
    pass

class PySNAPIException(PySNAPIBaseException):
    
    def __init__(self, *args: object, status_code=400) -> None:
        elapsed_mock = MagicMock()
        elapsed_mock.total_seconds.return_value = 10
        self.resp = MagicMock(status_code=status_code)
        self.resp.elapsed = elapsed_mock
        self.status_code = status_code
        super().__init__(*args)


class RequestException(Exception):
    pass
class RequestsDependencyWarning(Exception):
    pass