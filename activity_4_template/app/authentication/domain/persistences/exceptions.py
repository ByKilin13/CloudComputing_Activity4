class WrongPasswordException(Exception):
    pass


class UserNotFoundException(Exception):
    pass


class UsernameAlreadyTakenException(Exception):
    pass


class TokenNotFound(Exception):
    pass
