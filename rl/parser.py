import argparse
from torch.optim import Adam
from torch import nn
from models import ValueNetwork
import datetime
import os
from preprocess import simple_preprocess


def get_args():
    parser = argparse.ArgumentParser(description='DiDi')

    parser.add_argument(
        '--root-dir',
        default='./data',
        help='Example: --root-dir ./data')

    parser.add_argument(
        '--csv-file',
        type=str,
        default='rides_and_repositions.csv',
        help='Example: --csv-file filename.csv')

    parser.add_argument(
        '--cols',
        action='store', dest='cols',
        type=str, nargs='*', default=None,
        help='Example: --cols item1 item2 item3')

    parser.add_argument(
        '-sd', '--save-dir',
        default='./experiments/',
        help='Directory to save different experiments and common summaries (default: ./experiments/)')

    parser.add_argument(
        '-et', '--experiment-tag',
        default=None,
        help='tag of the current experiment. '
             'It affect name of the written summaries. '
             '(default: <current-time>)')

    parser.add_argument(
        '--state-cols',
        action='store', dest='state',
        type=str, nargs='*', default=['pickup_weekday', 'pickup_hour', 'pickup_lon', 'pickup_lat'],
        help='Example: --state-cols item1 item2 item3')

    parser.add_argument(
        '--next-state-cols',
        action='store', dest='next_state',
        type=str, nargs='*', default=['dropoff_weekday', 'dropoff_hour', 'dropoff_lon', 'dropoff_lat'],
        help='Example: --next-state-cols item1 item2 item3')

    parser.add_argument(
        '--reward-col', dest='reward',
        type=str, nargs='*', default=['reward'],
        help='Example: --reward-col item')

    parser.add_argument(
        '--info-cols', dest='info',
        type=str, nargs='*', default=[],
        help='Example: --info-cols item1 item2 item3')

    parser.add_argument(
        '--done-col', dest='done',
        type=str, nargs='*', default=['done'],
        help='Example: --done-col item')

    parser.add_argument(
        '--preprocess-fn',
        type=str, default='default',
        help='Example: --preprocess_fn preprocess')

    parser.add_argument(
        '--network',
        type=str, default="default",
        help='Example: --network network')

    parser.add_argument(
        '--batch-size',
        type=int, default=32,
        help='Example: --batch_size 32')

    parser.add_argument(
        '--gamma',
        type=float, default=0.92,
        help='Example: --gamma 0.99')

    parser.add_argument(
        '--device',
        type=str, default='cpu',
        help='Example: --device cuda')

    parser.add_argument(
        '--optimizer',
        type=str, default='default',
        help='Example: --device default')

    parser.add_argument(
        '--lr',
        type=float, default='1e-4',
        help='Example: --lr 1e-4')

    parser.add_argument(
        '--hidden-dim',
        type=int, default=128,
        help='Example: --hidden-dim 128')

    parser.add_argument(
        '--criterion',
        type=str, default='default',
        help='Example: --criterion mse')

    parser.add_argument(
        '--update',
        type=int, default=10,
        help='Example: --update 100')

    parser.add_argument(
        '--num-epochs',
        type=int, default=10,
        help='Example: --num-epochs 10')

    args = parser.parse_args()

    if args.preprocess_fn == 'default':
        args.preprocess_fn = simple_preprocess

    if args.optimizer == 'default':
        args.optimizer = Adam

    if args.network == 'default':
        args.network = ValueNetwork

    if args.criterion == 'default':
        args.criterion = nn.MSELoss()

    if not getattr(args, 'experiment_tag', None):
        date = datetime.datetime.now()
        args.experiment_tag = "{0.year}-{0.month}-{0.day}-{0.hour}-{0.minute}".format(date)

    args.summary_dir = os.path.join(args.save_dir, "summaries", args.experiment_tag)

    return args
