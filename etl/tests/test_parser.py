import pytest
import numpy as np
import pandas as pd
from src.main_parser import strip_namespace, normalise_ddd_to_mg

def test_strip_namespace():
    """Test that the XML namespace is correctly removed from the tag."""
    assert strip_namespace("{http://www.w3.org/2001/XMLSchema}tag") == "tag"
    assert strip_namespace("tag") == "tag"
    assert strip_namespace("{namespace}element") == "element"
    assert strip_namespace("") == ""

def test_normalise_ddd_to_mg():
    """Test that combinations of DDD values and UOMs output correctly parsed mg."""
    # Standard Conversions
    assert normalise_ddd_to_mg(1.5, "g") == 1500.0
    assert normalise_ddd_to_mg(500, "mg") == 500.0
    assert normalise_ddd_to_mg(250, "microgram") == 0.25
    assert normalise_ddd_to_mg(100, "mcg") == 0.1
    
    # Capitalization & spacing resilience
    assert normalise_ddd_to_mg(2.0, " G ") == 2000.0
    assert normalise_ddd_to_mg(3, "MG") == 3.0
    
    # Missing / NaN handling
    assert normalise_ddd_to_mg(pd.NA, "g") is None
    assert normalise_ddd_to_mg(np.nan, "g") is None
    assert normalise_ddd_to_mg(None, "g") is None
    
    # Unconvertible units
    assert normalise_ddd_to_mg(3, "MU") is None
    assert normalise_ddd_to_mg(1, "unit") is None
    assert normalise_ddd_to_mg(500, "ml") is None
