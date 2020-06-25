import torch
import torch.nn as nn
import torch.nn.functional as F


class ValueNetwork(nn.Module):
    def __init__(self, num_inputs, hidden_size):
        super(ValueNetwork, self).__init__()

        self.linear1 = nn.Linear(num_inputs, hidden_size)
        self.linear2 = nn.Linear(hidden_size, hidden_size)
        self.linear3 = nn.Linear(hidden_size, 1)

    def forward(self, state):
        x = F.relu(self.linear1(state))
        x = F.relu(self.linear2(x))
        x = self.linear3(x)
        return x


class EmbedNetwork(nn.Module):
    def __init__(self, num_inputs, hidden_size):
        super(EmbedNetwork, self).__init__()

        self.seconds = nn.Embedding(86400, hidden_size)
        self.dayofweek = nn.Embedding(7, hidden_size)
        self.dayofmonth = nn.Embedding(30, hidden_size)
        self.linear1 = nn.Linear(num_inputs, hidden_size)
        self.linear2 = nn.Linear(4*hidden_size, hidden_size)
        self.linear3 = nn.Linear(hidden_size, 1)

    def forward(self, state):
        seconds = state['cat'][:, 0]
        dayofweek = state['cat'][:, 1]
        dayofmonth = state['cat'][:, 2]
        seconds = self.seconds(seconds)
        dayofweek = self.dayofweek(dayofweek)
        dayofmonth = self.dayofmonth(dayofmonth)
        x = F.relu(self.linear1(state['cont']))
        x = torch.cat([x, seconds, dayofweek, dayofmonth], dim=1)
        x = F.relu(self.linear2(x))
        x = self.linear3(x)
        return x
