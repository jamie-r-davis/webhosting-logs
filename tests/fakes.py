class FakeLogEntry:
    def __init__(self, **kwargs):
        """Mock LogEntry object"""
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __getattr__(self, item: str):
        return "fake"
