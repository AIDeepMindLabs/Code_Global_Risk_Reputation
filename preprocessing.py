from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import json


class SubmodelTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, features=[], pairs_of_interest=[],
                 extra_features=['sessionid', 'accountid', 'unixtime',
                                 'fraudlistentry', 'fraud-discount',
                                 '_artificial_index_'
                                 ]):
        self.features = extra_features + features
        self.pairs_of_interest = pairs_of_interest
    
    def fit(self, df, y=None):
        keys = df.keys()
        self.features = [f for f in self.features if f in keys]
        self.pairs_of_interest = [(a, b) for a, b in self.pairs_of_interest
                                  if a in keys and b in keys]

        return self

    def transform(self, df):
        ret = df[self.features].copy()
        for a, b in self.pairs_of_interest:
            ret[a + '::' + b] = df[a] + '|||' + df[b]

        return ret


class EndpointTransformer(SubmodelTransformer):
    def __init__(self):
        features = ['browserlanguage', 'useragent', 'deviceid', 'device_type',
                    'browserplatform', 'browserparent', 'browsername',
                    'device_pointing_method', 'city', 'country', 'region',
                    'ip']
        pairs_of_interest = [('browserparent', 'device_type'),
                             ('browserparent', 'browserplatform'),
                             ('useragent', 'deviceid'),
                             ('device_type', 'useragent'),
                             ('device_type', 'device_pointing_method'),
                             ('device_type', 'ip'),
                             ('device_type', 'city')]

        super(EndpointTransformer, self).__init__(features, pairs_of_interest)


class ShippingTransformer(SubmodelTransformer):
    def __init__(self):
        features = ['shipping_info']
        super(ShippingTransformer, self).__init__(features)

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        ret = super(ShippingTransformer, self).transform(X)
        
        new_info = []
        for info in ret.shipping_info:
            try:
                info = json.loads(info)
            except:
                info = {}

            shipping_address = info.get('ShippingAddress', {})
            
            next_ = {v: shipping_address.get(v, None)
                     for v in ['shippingcountry', 'shippingzipcode',
                               'shippingnamelast', 'shippingnamefirst',
                               'shippingstate', 'shippingphonenumber',
                               ]}
            next_['shippingstreet'] = shipping_address.get('shippingstreet',
                                                           '') + ' ' + \
                shipping_address.get('shippingstreet2', '')

            next_['billingzipcode'] = info.get('BillingAddress',
                                               {}).get('billingzipcode', None)
            new_info.append(next_)

        new_info = pd.DataFrame(new_info)
        ret = ret.join(new_info)
        
        return ret


class PurchaseTransformer(SubmodelTransformer):
    def __init__(self):
        features = ['cart_info']
        super(PurchaseTransformer, self).__init__(features)

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        def next_total(p):
            try:
                return float(p.get('productPrice', 0)) * \
                    float(p.get('productQuantity', 0))
            except:
                return 0.

        ret = super(PurchaseTransformer, self).transform(X)
        
        new_info = []
        for info in ret.cart_info.values:
            try:
                info = json.loads(info)
            except:
                info = {}

            info = info.get('CartProduct', {})
            products = []
            if 'all' in info:
                products = info['all']

            amount = sum([next_total(p) for p in products])
            categorical_amount = 'xl'
            if amount < 800.:
                categorical_amount = 's'
            elif amount < 1000.:
                categorical_amount = 'm'
            elif amount < 3000.:
                categorical_amount = 'l'


            types = [p.get('productType', None) for p in products]
            types = ','.join([p for p in types if p is not None])
            
            next_ = {'cart-categorical-amount': categorical_amount,
                     'cart-types': types
                     }
            new_info.append(next_)

        new_info = pd.DataFrame(new_info)
        ret = ret.join(new_info)

        return ret


class FraudTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, fraud_filename='data/fraud_list.csv'):
        fraud_list = pd.read_csv(fraud_filename)
        self.fraud_email = set(fraud_list['customer_email'].values)
        self.fraud_ip = set(fraud_list['ip'].values)

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        fraud_accountid = [a in self.fraud_email
                           for a in X['accountid'].values]
        fraud_ip = [1 - 0.0 * int(a in self.fraud_ip) for a in X['ip'].values]

        ret = X.copy()
        ret['fraudlistentry'] = fraud_accountid
        ret['fraud-discount'] = fraud_ip

        return ret

