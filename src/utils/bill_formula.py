from random import uniform, choices

# Usage dict
usage_dict = {
    1: 500, # Wh: Divide 1000 (10**3) -> kWh
    2: 166, # Wh: Divide 1000 (10**3) -> kWh
    3: 6250 # CuCM: Divide 1000000 (10**6) -> CuM
}

# Cost dict
cost_dict = {
    1: 1045, # Divide 10000 (10**4) -> Dollars
    2: 333, # Divide 10000 (10**4) -> Dollars
    3: 145, # Divide 10000 (10**4) -> Dollars
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
def calc_usage(row, type):
    base = usage_dict[type]
    return int(base * ht_multiplier(row['housing_type']) * hm_multiplier(row['household_members']) * ab_multiplier())

# Calculate bill
def calc_bill(row, type):
    base = cost_dict[type]
    return int(base * ht_multiplier(row['housing_type']) * hm_multiplier(row['household_members']) * ab_multiplier())