"""Errors raised by the fake external services (what a real client library would raise)."""


class ServiceError(Exception):
    """Base class for errors reported by an external service."""


class PaymentDeclinedError(ServiceError):
    """The payment provider refused the request. Nothing happened, so retrying is safe."""


class IdempotencyKeyReuseError(ServiceError):
    """The same idempotency key was sent with different parameters."""


class EmailDeliveryError(ServiceError):
    """The email provider refused the message. Nothing was sent."""


def connection_loss_is_ambiguous(fn):
    """A fake provider whose own backing store is unreachable behaves like a real provider whose
    network connection dropped: the caller cannot know whether the request was processed."""
    import functools

    from sqlalchemy.exc import SQLAlchemyError

    from app.engine.errors import AmbiguousOutcomeError

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        try:
            return await fn(*args, **kwargs)
        except (SQLAlchemyError, ConnectionError, TimeoutError) as exc:
            if isinstance(exc, SQLAlchemyError) and "IntegrityError" in type(exc).__name__:
                raise
            raise AmbiguousOutcomeError(f"connection to the provider was lost: {type(exc).__name__}") from exc

    return wrapper
