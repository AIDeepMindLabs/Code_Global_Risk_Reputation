from sklearn.base import BaseEstimator, RegressorMixin
from collections import Counter
import pandas as pd
import numpy as np
import json
import os


import utils


"""
The score assigned by the endpoint model is a weighted average of the
score assigned to each feature. The score is the normalized probability of
expressing the current value for each feature, namely, the user's probability
for that feature-value pair divided by the probability of the most probable
value. Then, we regularize the score by computing the "rareness" of that value.
Thereby, if most of the users are from the United States, and a user buys
from Canada, which is a minority section of the population, the impact of
a rare behavior on that attribute such be more important for that user.

The relevance of each feature is defined by the entropy of the feature.
Thereby, a feature with no variance has 0 impact on the final model, while a
feature with strong diversity is very important on the decision process.
"""

class EndpointScorer(BaseEstimator, RegressorMixin):
    def __init__(self, universe_prior=0.25):
        self.skip_features = set(['sessionid', 'accountid', 'unixtime',
                                  '_artificial_index_'])
        self.epsilon = 1e-10
        self.universe_prior = universe_prior

    def fit(self, df, y=None):
        self.global_frequencies = {}
        for f in df.keys():
            if f in self.skip_features:
                continue

            acc_f = ['accountid', f]
            values = df[acc_f].groupby(acc_f).size().reset_index().rename(
                columns={0:'count'})[f]
            
            self.global_frequencies[f] = {i: float(v) / len(values)
                                          for i, v in Counter(values).items()}
        
        self.ft_relevance = \
            {f: 1 - np.mean([v for _, v in self.global_frequencies[f].items()])
             for f in df.keys() if f not in self.skip_features}
        rel_sum = np.sum([v for _, v in self.ft_relevance.items()])
        self.ft_relevance = \
            {f: v / rel_sum for f, v in self.ft_relevance.items()}

        return self

    def predict(self, X):
        ordered_X = X.copy().sort_values(['accountid', 'unixtime'], axis=0)
        accounts = ordered_X['accountid'].values

        tid = 0
        ret = []
        while tid < ordered_X.shape[0]:
            next_tid = tid
            current_account = accounts[next_tid]
            while (next_tid < ordered_X.shape[0] and
                   accounts[next_tid] == current_account):
                # Process the next transaction
                account_transations = ordered_X[tid: next_tid + 1]
                transaction = account_transations.iloc[next_tid - tid]
                score = self.predict_transactions(account_transations)
                # Append the result
                ret.append({'sessionid': transaction['sessionid'],
                            'accountid': current_account,
                            'fraudlistentry': transaction.get('fraudlistentry',
                                                              0),
                            'fraud-discount': transaction.get('fraud-discount',
                                                              1.),
                            'endpointscore': score * 100.,
                            '_artificial_index_': 
                                transaction['_artificial_index_'],
                            'num_transactions': len(account_transations)
                            })
                next_tid += 1
            tid = next_tid

        ret = pd.DataFrame(ret)

        return ret

    def predict_transactions(self, transactions):
        if transactions.shape[0] <= 1:
            return 1.

        user_frequencies = utils.feature_frequencies(transactions)
        last_transaction = transactions.iloc[-1]

        relevances = []
        for feature in transactions.keys():
            if (feature in self.skip_features or
                feature not in user_frequencies or
                feature not in self.global_frequencies):
                continue

            if feature not in user_frequencies:
                continue

            ufreq = user_frequencies.get(feature, self.epsilon)
            max_ufreq = np.max([v
                                for _, v in user_frequencies[feature].items()])
            gfreq = self.global_frequencies.get(feature, self.epsilon)

            value = last_transaction[feature]
            if type(value) is float and np.isnan(value):
                continue

            ft_rel = self.ft_relevance[feature]
            ft_match = ((ufreq[value] / max_ufreq) * 
                        ((1 - self.universe_prior) +
                         self.universe_prior * (1 - gfreq[value])))

            relevances.append((ft_match, ft_rel))

        relevances = np.asarray(relevances)
        ret = np.average(relevances[:, 0], weights=relevances[:, 1])

        if len(transactions) == 2:
            ret = ret * 0.75

        return ret


"""
The score assigned by the shipping model is a weighted average of the
probability of having the current transaction value for each feature. In order
to avoid punishing users with multimodal behaviors (e.g. using two zipcodes),
we compute the normalized probability for the features, namely, the
probability of having the current value divided by the probability of the
most probable value.
"""

class ShippingScorer(BaseEstimator, RegressorMixin):
    def __init__(self):                        
        self.relevance = {
            'shippingcountry': 2,
            'shippingzipcode': 2,
            'shippingstreet': 1,
            'shippingstate': 2.5,
            'shippingphonenumber': 0.5,
            'shippingnamefirst': 0.5,
            'shippingnamelast': 0.5,
            'billingzipcode': 1
            }

    def fit(self, X, y=None):
        return self

    def predict(self, X):
        ordered_X = X.copy().sort_values(['accountid', 'unixtime'], axis=0)
        accounts = ordered_X['accountid'].values

        tid = 0
        ret = []
        while tid < ordered_X.shape[0]:
            next_tid = tid
            current_account = accounts[next_tid]
            while (next_tid < ordered_X.shape[0] and
                   accounts[next_tid] == current_account):
                # Process the next transaction
                account_transations = ordered_X[tid: next_tid + 1]
                transaction = account_transations.iloc[next_tid - tid]
                score = self.predict_transactions(account_transations)
                # Append the result
                ret.append({'sessionid': transaction['sessionid'],
                            'accountid': current_account,
                            'fraudlistentry': transaction.get('fraudlistentry',
                                                              0),
                            'fraud-discount': transaction.get('fraud-discount',
                                                              1.),
                            'shippingscore': score * 100.,
                            '_artificial_index_': 
                                transaction['_artificial_index_'],
                            'num_transactions': len(account_transations)})
                next_tid += 1
            tid = next_tid

        ret = pd.DataFrame(ret)
        return ret

    def predict_transactions(self, transactions):
        if len(transactions) <= 1:
            return 1.

        user_frequencies = utils.feature_frequencies(transactions)

        relevances = []
        for f, r in self.relevance.items():
            value = transactions[f].iloc[-1]
            prob = user_frequencies[f].get(value, 0)
            max_prob = np.max([v for _, v in user_frequencies[f].items()])
            prob /= max_prob
            
            relevances.append((prob, r))

        relevances = np.asarray(relevances)

        ret = np.average(relevances[:, 0], weights=relevances[:, 1])

        if len(transactions) == 2:
            ret = ret * 0.75

        return ret

    
"""
The score assigned by the purchase model is a weighted average of the
cart size and the user's history and intersection between the cart.
"""


class PurchaseScorer(BaseEstimator, RegressorMixin):
    def __init__(self, window=10):                        
        self.window = window

    def fit(self, X, y=None):
        return self

    def predict(self, X):
        ordered_X = X.copy().sort_values(['accountid', 'unixtime'], axis=0)
        accounts = ordered_X['accountid'].values

        tid = 0
        ret = []
        while tid < ordered_X.shape[0]:
            next_tid = tid
            current_account = accounts[next_tid]
            while (next_tid < ordered_X.shape[0] and
                   accounts[next_tid] == current_account):
                # Process the next transaction
                account_transations = ordered_X[tid: next_tid + 1]
                transaction = account_transations.iloc[next_tid - tid]
                score = self.predict_transactions(account_transations)
                # Append the result
                ret.append({'sessionid': transaction['sessionid'],
                            'accountid': current_account,
                            'fraudlistentry': transaction.get('fraudlistentry',
                                                              0),
                            'fraud-discount': transaction.get('fraud-discount',
                                                              1.),
                            'purchasescore': score * 100.,
                            '_artificial_index_': 
                                transaction['_artificial_index_'],
                            'num_transactions': len(account_transations)})
                next_tid += 1
            tid = next_tid

        ret = pd.DataFrame(ret)

        return ret

    def predict_transactions(self, transactions):
        if len(transactions) <= 1:
            return 1.
        
        ret = 0.

        current = transactions.iloc[-1]
        cat_freq = Counter(transactions['cart-categorical-amount'])
        max_cat_freq = float(max([v for _, v in cat_freq.items()]))
        cat_rel_freq = float(cat_freq[current['cart-categorical-amount']]) / \
            max_cat_freq
        
        ret += 2. * cat_rel_freq

        transactions = transactions.iloc[-self.window - 1:]
        
        all_types = transactions['cart-types'].iloc[: -1].values
        all_types = set(','.join(all_types).split(','))
        last_types = set(current['cart-types'].split(','))
        types_intersect = float(len(all_types & last_types)) / len(last_types)
        
        ret += 1.5 * types_intersect
        
        ret = ret / 4.5

        if len(transactions) == 2:
            ret = ret * 0.75

        return ret


"""
This class implements a merger of the models' scores.
It receives a list of names, models, and weights to be used in the weighted
average score. It applies a disccount on the fraud transactions.
"""

class ModelMerger(BaseEstimator, RegressorMixin):
    def __init__(self, estimators):
        self.estimators = estimators
        self.names = [n for n, _, _ in estimators]
        self.weights = [w for _, _, w in estimators]

    def fit(self, X, y=None):
        self.estimators = [(name, estimator.fit(X, y), weight)
                           for name, estimator, weight in self.estimators]
        return self

    def predict(self, X):
        ret = None
        for name, estimator, weight in self.estimators:
            preds = estimator.predict(X)
            next_ = preds[['_artificial_index_', 'sessionid', 'accountid',
                           'num_transactions', name]]

            # Aggregate the scores
            if ret is None:
                ret = next_.copy()
                finalscore = next_[name].values * weight * \
                    preds['fraud-discount'].values
                ret['finalscore'] = finalscore
                ret['fraudlistentry'] = preds['fraudlistentry'].values

                signals = ['' if fd == 1. else 'Fraudulent IP'
                           for fd in preds['fraud-discount']]
                ret['signalstriggered'] = np.asarray(signals)
            else:
                ret[name] = next_[name]
                ret['finalscore'] += next_[name].values * weight * \
                    preds['fraud-discount'].values

        # Compute the average score
        ret['finalscore'] /= np.sum(self.weights)

        # Compute the final band
        band = ret['finalscore'].values.copy() / 100.
        band = 1 + 4. * (1. - band)
        ret['finalband'] = band

        return ret

    def transform(self, X):
        return self.predict(X)
