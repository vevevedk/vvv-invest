"""
Alert thresholds configuration
"""

# Institutional Flow Thresholds
PREMIUM_THRESHOLD = 100000  # Minimum premium for institutional trade ($)
ZSCORE_THRESHOLD = 2.0  # Z-score threshold for unusual activity
VOLUME_THRESHOLD = 1000  # Minimum volume for institutional trade

# Strike Concentration Thresholds
CONCENTRATION_THRESHOLD = 0.1  # 10% of total premium
STRIKE_CLUSTER_SIZE = 3  # Minimum number of strikes in a cluster

# Put/Call Ratio Thresholds
RATIO_CHANGE_THRESHOLD = 0.2  # 20% change in ratio
SIGNIFICANT_RATIO_CHANGE = 0.3  # 30% change for significant shift
MIN_PREMIUM_FOR_RATIO = 50000  # Minimum premium for ratio calculation

# Dark Pool Thresholds
DARK_POOL_VALUE_THRESHOLD = 50000  # Minimum value for dark pool trade
DARK_POOL_VOLUME_THRESHOLD = 1000  # Minimum volume for dark pool trade 