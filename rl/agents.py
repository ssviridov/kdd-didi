import torch
from torch import nn
import numpy as np
import torch.optim as optim


class BaseAgent:

    def dispatch(self):
        pass

    def reposition(self):
        pass

    def train(self):
        pass

    def save(self, path):
        pass


class ValueAgent(BaseAgent):

    def __init__(self, value_net_class,
                 replay_buffer,
                 batch_size=32,
                 gamma=0.99,
                 device="cpu",
                 optimizer=optim.Adam,
                 lr=1e-3,
                 state_dim=6,
                 hidden_dim=256,
                 criterion=nn.MSELoss(),
                 update=10):
        self.device = device
        self.value_net = value_net_class(state_dim, hidden_dim).to(self.device)
        self.target_value_net = value_net_class(state_dim, hidden_dim).to(self.device)

        for target_param, param in zip(self.target_value_net.parameters(), self.value_net.parameters()):
            target_param.data.copy_(param.data)

        self.replay_buffer = replay_buffer
        self.batch_size = batch_size
        self.gamma = gamma
        self.optimizer = optimizer(self.value_net.parameters(), lr=lr)
        self.criterion = criterion
        self.update = update
        self.iter = 0

    def train(self):
        state, reward, next_state, info = self.replay_buffer.sample(self.batch_size)

        state = torch.FloatTensor(state).to(self.device)
        reward = torch.FloatTensor(reward).unsqueeze(1).to(self.device)
        next_state = torch.FloatTensor(next_state).to(self.device)
        k = torch.FloatTensor(info).unsqueeze(1).to(self.device)

        value = self.value_net(state)
        target_value = (reward*((self.gamma**k) - 1))/(k*(self.gamma - 1)) +\
                       (self.gamma**k) * self.target_value_net(next_state)

        value_loss = self.criterion(value, target_value.detach())

        self.optimizer.zero_grad()
        value_loss.backward()
        self.optimizer.step()

        self.iter += 1

        if self.iter % self.update:
            for target_param, param in zip(self.target_value_net.parameters(), self.value_net.parameters()):
                target_param.data.copy_(param.data)

        return value_loss.detach().item()

    def save(self, path):
        torch.save(self.value_net, path)
