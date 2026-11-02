-- Fake payment/email services get their own database so their ledgers are
-- independent of the runtime's event log (see Feature 11).
CREATE DATABASE fake_services OWNER durable;
