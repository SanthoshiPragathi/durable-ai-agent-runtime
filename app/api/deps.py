from app.database.session import SessionFactory, get_default_session_factory


def get_session_factory() -> SessionFactory:
    """FastAPI dependency. Tests override it to point at the test database."""
    return get_default_session_factory()


def get_services_session_factory_dep() -> SessionFactory:
    """FastAPI dependency for the fake services' own database. Tests override it."""
    from app.integrations.tools.fake_ledger import get_services_session_factory

    return get_services_session_factory()
