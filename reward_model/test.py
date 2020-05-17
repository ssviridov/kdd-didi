import pickle
import os

from reward_model import RewardModel

cur_dir = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(cur_dir, 'src', 'test_request.pickle'), 'rb') as f:
    request = pickle.load(f)

if __name__=='__main__':
    model = RewardModel()
    reward_units = model.predict(request)
    print(f'Predicted reward_units {reward_units}')