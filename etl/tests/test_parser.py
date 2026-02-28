import pytest
from src.main_parser import strip_namespace

def test_strip_namespace():
    """Test that the XML namespace is correctly removed from the tag."""
    assert strip_namespace("{http://www.w3.org/2001/XMLSchema}tag") == "tag"
    assert strip_namespace("tag") == "tag"
    assert strip_namespace("{namespace}element") == "element"
    assert strip_namespace("") == ""
