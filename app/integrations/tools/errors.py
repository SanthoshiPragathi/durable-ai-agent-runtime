"""Errors raised by the fake external services (what a real client library would raise)."""


class ServiceError(Exception):
    """Base class for errors reported by an external service."""


class PaymentDeclinedError(ServiceError):
    """The payment provider refused the request. Nothing happened, so retrying is safe."""


class IdempotencyKeyReuseError(ServiceError):
    """The same idempotency key was sent with different parameters."""


class EmailDeliveryError(ServiceError):
    """The email provider refused the message. Nothing was sent."""
