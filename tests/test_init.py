"""Test package initialization."""


def test_import_fedledger():
    """Test that the package can be imported."""
    import fedledger
    assert fedledger.__version__ == "0.1.0"


def test_import_all_modules():
    """Test that all modules can be imported."""
    from fedledger import cli
    from fedledger import config
    from fedledger import ids
    from fedledger import models
    from fedledger import http
    from fedledger import logging_config
    from fedledger import pydantic_models
    from fedledger import schema
    from fedledger import pipeline
    
    # All imports should succeed
    assert cli is not None
    assert config is not None
    assert ids is not None
    assert models is not None
    assert http is not None
    assert logging_config is not None
    assert pydantic_models is not None
    assert schema is not None
    assert pipeline is not None
