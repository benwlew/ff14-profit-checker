import pytest
from market import split_listings_by_quality, filter_outliers, calculate_market_stats

def test_split_listings_by_quality(sample_listings):
    """Test that listings are correctly split by quality."""
    nq_listings, hq_listings = split_listings_by_quality(sample_listings)
    
    # Check counts (excluding mannequin listing)
    assert len(hq_listings) == 3
    assert len(nq_listings) == 3
    
    # Verify all NQ listings are actually NQ
    assert all(not listing['hq'] for listing in nq_listings)
    
    # Verify all HQ listings are actually HQ
    assert all(listing['hq'] for listing in hq_listings)

def test_filter_outliers():
    """Test outlier filtering with known values."""
    # Test case with clear outliers
    prices = [100, 110, 120, 500, 105, 115, 1000]
    filtered = filter_outliers(prices)
    
    # Verify outliers were removed
    assert 1000 not in filtered
    assert 500 not in filtered
    assert len(filtered) == 5
    
    # Test empty list
    assert filter_outliers([]) is None
    
    # Test single value
    single_price = [100]
    assert filter_outliers(single_price) == single_price
    
    # Test no outliers case
    normal_prices = [100, 105, 110, 115]
    assert filter_outliers(normal_prices) == normal_prices

def test_calculate_market_stats(sample_listings):
    """Test market statistics calculation."""
    stats = calculate_market_stats(sample_listings)
    
    # Check that we have both HQ and NQ stats
    assert 'hq' in stats
    assert 'nq' in stats
    
    # Check HQ stats
    hq_stats = stats['hq']
    assert 'medianPrice' in hq_stats
    assert 'minPrice' in hq_stats
    assert len(hq_stats['listings']) <= 5  # Should have max 5 listings
    assert hq_stats['total_listings'] == 3  # Excluding mannequin
    
    # Check NQ stats
    nq_stats = stats['nq']
    assert 'medianPrice' in nq_stats
    assert 'minPrice' in nq_stats
    assert len(nq_stats['listings']) <= 5
    assert nq_stats['total_listings'] == 3
    
    # Verify min prices are correct
    assert hq_stats['minPrice'] == 950  # Lowest non-mannequin HQ price
    assert nq_stats['minPrice'] == 750  # 100 should be filtered as outlier
    
    # Test empty listings
    assert calculate_market_stats([]) is None