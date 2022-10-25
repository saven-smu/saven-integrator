import pandas as pd

# def normalize_stg1(sr)
#     return (sr-sr.mean())/sr.std()

htd_dict = {
    "hdb1": 1.4,
    "hdb2": 1.25,
    "hdb3": 1.1,
    "hdb4": 1,
    "hdb5": 0.9,
    "bg": 0.75,
    "sd": 0.8,
    "tr": 0.85,
    "cm": 0.9,
    "ecm": 0.9,
    "sh": 0.85
}

def normalize(values, bounds):
    return [1000 + (x - bounds['lower']) * (9000) / (bounds['upper'] - bounds['lower']) for x in values]

def proc_df(df, lbid):
    # Set lid
    df["leaderboard_id"] = lbid

    # Generate score
    scores = df["norm_usage"].tolist()
    bounds = {'lower': min(scores), 'upper': max(scores)}
    df["computed_score"] = pd.Series(normalize(scores, bounds))
    
    # Sort and assign positions
    df.sort_values(by=['computed_score'], ascending=False, inplace=True)
    df.reset_index(drop=True)
    df['position'] = pd.Series(list(range(1, len(df.index) + 1)))

    return df[['position', 'computed_score', 'user_id', 'leaderboard_id']]
