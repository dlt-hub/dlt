def pytest_configure(config):
    config.addinivalue_line(
        "markers", "no_registration_fixture: marks test to not use fsspec registration fixture"
    )
