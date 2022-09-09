class FileTypeError(Exception):
    """Exception raised if a file is not of the right type"""

    pass


class DomainContinuityError(Exception):
    """Exception raised if a particular domain doesn't have continuity in its logs for the given timespan"""
