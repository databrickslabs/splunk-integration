class Response:
    """Sample Response Class."""

    def __init__(self, status_code):
        """Init Method for Response."""
        self.status_code = status_code
        
    def json(self):
        """Set json value."""
        return {"status_code": self.status_code}