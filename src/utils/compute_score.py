from random import randint

htd_dict = {
    "hdb1": 1.8,
    "hdb2": 1.6,
    "hdb3": 1.4,
    "hdb4": 1.2,
    "hdb5": 1.0,
    "bg": 0.65,
    "sd": 0.7,
    "tr": 0.75,
    "cm": 0.9,
    "ecm": 0.9,
    "sh": 0.75
}

def normalize(values, bounds):
    arb = randint(2666, 2999)
    return [int(arb + (x - bounds['lower']) * (7000) / (bounds['upper'] - bounds['lower'])) for x in values]

def reverse_mm_normalize(srs):
    return 1 - (srs-srs.min())/(srs.max()-srs.min())

def proc_df(df, lbid):
    # Set lid
    df["leaderboard_id"] = lbid

    # Generate score
    scores = df["norm_usage"].tolist()
    bounds = {'lower': min(scores), 'upper': max(scores)}
    df["computed_score"] = normalize(scores, bounds)
    
    # Sort and assign positions
    df.sort_values(by=['computed_score'], ascending=False, inplace=True)
    df['position'] = list(range(1, len(df.index) + 1))

    return df[['position', 'computed_score', 'user_id', 'leaderboard_id']]
