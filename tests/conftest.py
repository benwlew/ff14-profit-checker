import pytest
import os
import sys

# Add the project root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

@pytest.fixture
def sample_listings():
    """Fixture providing sample market listings for testing."""
    return [
        {"pricePerUnit": 1000, "hq": True, "onMannequin": False},
        {"pricePerUnit": 950, "hq": True, "onMannequin": False},
        {"pricePerUnit": 1200, "hq": True, "onMannequin": False},
        {"pricePerUnit": 800, "hq": False, "onMannequin": False},
        {"pricePerUnit": 750, "hq": False, "onMannequin": False},
        {"pricePerUnit": 2000, "hq": True, "onMannequin": True},  # Should be filtered out
        {"pricePerUnit": 100, "hq": False, "onMannequin": False},  # Outlier
    ]

@pytest.fixture
def sample_recipe():
    """Fixture providing a sample recipe for testing."""
    return {
        "ID": 1234,
        "AmountResult": 1,
        "ItemResult": 5678,
        "ItemIngredient0": 1111,
        "AmountIngredient0": 2,
        "ItemIngredient1": 2222,
        "AmountIngredient1": 1,
    }