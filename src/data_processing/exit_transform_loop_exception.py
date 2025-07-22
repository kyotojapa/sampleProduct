"Module to hold the exit transform loop exception"


class ExitTransformLoopException(Exception):
    "ExitTransformLoopException"

    def __init__(self, message="This message not used for transforms"):
        super().__init__(message)
