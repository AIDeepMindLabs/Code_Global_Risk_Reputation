from sklearn.pipeline import Pipeline
import pandas as pd
import numpy as np
import argparse
import os


from preprocessing import EndpointTransformer, ShippingTransformer, \
    PurchaseTransformer, FraudTransformer
from models import EndpointScorer, ShippingScorer, PurchaseScorer, ModelMerger


def get_args():
    parser = argparse.ArgumentParser(
                        description="Fraud detection",
                        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--data', metavar="D", nargs='?',
                        default="", help='Path to the data file')
    parser.add_argument('--fraud-list', metavar="F", nargs='?',
                        default="data/fraud_list.csv",
                        help='Path to the fraud file')
    parser.add_argument('--output', metavar="O", nargs='?',
                        default="output.csv", help='Path to the output file')
    parser.add_argument('--endpoint-model', metavar="E", nargs='?', type=int,
                        default=1, help='Use Endpoint model (1-0)')
    parser.add_argument('--shipping-model', metavar="S", nargs='?', type=int,
                        default=1, help='Use Shipping model (1-0)')
    parser.add_argument('--purchase-model', metavar="P", nargs='?', type=int,
                        default=1, help='Use Purchase model (1-0)')
    parser.add_argument('--csv-delimiter', metavar="CD", nargs='?',
                        default=',',
                        help='Delimiter used for parsing the input CSV')
    return parser.parse_args()


args = get_args()

filename = args.data
fraud_list = args.fraud_list

# Load the input data
df = pd.read_csv(filename, sep=args.csv_delimiter)
df['_artificial_index_'] = np.arange(len(df))

# Load the submodels
models = []
if args.endpoint_model != 0:
    endpoint_model = Pipeline([('features', EndpointTransformer()),
                               ('model', EndpointScorer())])
    models.append(('endpointscore', endpoint_model, 0.25))
if args.shipping_model != 0:
    shipping_model = Pipeline([('features', ShippingTransformer()),
                               ('model', ShippingScorer())])
    models.append(('shippingscore', shipping_model, 0.50))
if args.purchase_model != 0:
    purchase_model = Pipeline([('features', PurchaseTransformer()),
                               ('model', PurchaseScorer())])
    models.append(('purchasescore', purchase_model, 0.25))

# Merge the models and add the fraud signals
full_model = Pipeline([('fraud', FraudTransformer()),
                       ('models', ModelMerger(models))
                       ])

# Fit the model and compute the predictions
full_model.fit(df)
ret = full_model.predict(df)
ret = ret.sort_values(['_artificial_index_'], axis=0)

# Apply the signals penalties
signals_of_interest = ['account_create_velocity', 'account_testing',
                       'event_velocity', 'geo_anonymous', 'input_anomaly',
                       'input_scripted', 'login_accounts', 'login_failure',
                       'login_velocity', 'net_anomaly_ip', 'net_anomaly_ua',
                       'shiptobill_distance']
signals_of_interest = set(signals_of_interest)

all_signals = set([])
triggered_signals = []
deductions = []
for s, other in zip(df.eventtriggeredsignals.values,
                    ret['signalstriggered'].values):
    s = [x.replace('"', '') for x in s[1: -1].split(',')]
    s = set(s) & signals_of_interest
    text_s = other + ', '.join(set(s))
    triggered_signals.append(text_s)

    if 'geo_anonymous' in s:
        deductions.append(0.6 - (0.1 if len(s) > 1 else 0.0))
    else:
        deductions.append(max(0.6, 1. - 0.1 * len(s)))
ret['signalstriggered'] = triggered_signals

# Correct the final score and final band
ret['finalscore'] *= np.asarray(deductions)

band = ret['finalscore'].values.copy() / 100.
band = 1 + 4. * (1. - band)
ret['finalband'] = band 


# Print the output
ret.to_csv(args.output, sep=',',
           columns=['sessionid', 'accountid', 'endpointscore',
                    'purchasescore', 'shippingscore', 'finalscore',
                    'finalband', 'fraudlistentry', 'signalstriggered'],
           index=False, quotechar='"')
