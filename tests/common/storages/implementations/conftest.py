def pytest_configure(config):
    config.addinivalue_line(
        "markers", "skip_fsspec_registration: marks test to not use fsspec registration fixture"
    )
