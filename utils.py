from collections import Counter

def feature_frequencies(df):
    def next_freq(f):
        size = df.shape[0]
        return dict([(k, float(v) / size)
                     for k, v in Counter(df[f].values).items()])

    keys_ = df.keys()
    return {f: next_freq(f) for f in keys_}
