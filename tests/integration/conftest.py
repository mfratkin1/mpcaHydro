# -*- coding: utf-8 -*-
"""Pytest configuration for integration tests."""

import pytest


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test (requires full dependencies)"
    )
    config.addinivalue_line(
        "markers", "network: mark test as requiring network access"
    )
    config.addinivalue_line(
        "markers", "credentials: mark test as requiring external credentials"
    )
