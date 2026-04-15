class InvalidCSVError(Exception):
    """Raised when a file fails CSV validation —
    either by extension check or content sniffing."""
    pass