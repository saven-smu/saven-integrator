from random import uniform, choices

# Usage dict
usage_dict = {
    1: 500, # Wh: Divide 1000 (10**3) -> kWh
    2: 166, # Wh: Divide 1000 (10**3) -> kWh
    3: 6250 # CuCM: Divide 1000000 (10**6) -> CuM
}

# Range of housing type multiplier
htm_dict = {
    "hdb1": (0.35, 0.5),
    "hdb2": (0.5, 0.65),
    "hdb3": (0.65, 0.8),
    "hdb4": (0.9, 1.1),
    "hdb5": (1.2, 1.35),
    "bg": (2.5, 4.0),
    "sd": (1.8, 3.5),
    "tr": (1.5, 3.0),
    "cm": (0.8, 1.5),
    "ecm": (0.8, 1.5),
    "sh": (1.5, 3.0),
}

# Range of abnormally multiplier
abm_dict = {
    1: (0.2, 5),
    2: (0.5, 2),
    3: (0.8, 1.5),
    4: (1, 1),
}

# Range of hourly multiplier
hour_dict = {
    0: (0, 0.25),
    1: (0, 0.1),
    2: (0, 0.05),
    3: (0, 0.05),
    4: (0, 0.1),
    5: (0, 0.25),
    6: (0.1, 0.85),
    7: (0.6, 1),
    8: (1, 1.5),
    9: (1, 1.5),
    10: (0.75, 1),
    11: (0.25, 0.5),
    12: (0.4, 0.7),
    13: (0.3, 0.6),
    14: (0.25, 0.5),
    15: (0.15, 0.4),
    16: (0.25, 0.5),
    17: (0.55, 0.75, ),
    18: (0.8, 0.9),
    19: (0.9, 1.1),
    20: (1, 1.2),
    21: (1.1, 1.2),
    22: (0.8, 1),
    23: (0.5, 0.65),
}

# Hourly Multiplier
def hr_multiplier(hour):
    multi_rg = hour_dict[hour]
    return uniform(multi_rg[0], multi_rg[1])

# Housing type Multiplier
def ht_multiplier(housing_type):
    multi_rg = htm_dict[housing_type.lower()]
    return uniform(multi_rg[0], multi_rg[1])

# Housing member Multiplier
def hm_multiplier(household_members):
    multi_low = (household_members/4) * 0.85
    multi_high = (household_members/4) * 1.15
    return uniform(multi_low, multi_high)

# Abnormally Multiplier
def ab_multiplier():
    ab_list = [1,2,3,4]
    multi_rg = abm_dict[choices(ab_list, weights=(1, 5, 10, 1000), k=1)[0]]
    return uniform(multi_rg[0], multi_rg[1])

# Calculate usage
def calc_usage(row, type, hour):
    base = usage_dict[type]
    return int(base * ht_multiplier(row['housing_type']) * hm_multiplier(row['household_members']) * ab_multiplier() * hr_multiplier(hour))
