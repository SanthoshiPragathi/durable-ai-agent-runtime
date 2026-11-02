from app.database.session import SessionFactory, get_default_session_factory


def get_session_factory() -> SessionFactory:
    """FastAPI dependency. Tests override it to point at the test database."""
    return get_default_session_factory()
