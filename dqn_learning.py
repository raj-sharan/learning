import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
import random
from collections import deque

# Define DQN network
class DQNetwork(nn.Module):
    def __init__(self, state_size, action_size):
        super(DQNetwork, self).__init__()
        self.fc1 = nn.Linear(state_size, 64)
        self.fc2 = nn.Linear(64, 64)
        self.out = nn.Linear(64, action_size)

    def forward(self, x):
        x = torch.relu(self.fc1(x))
        x = torch.relu(self.fc2(x))
        return self.out(x)

# Replay memory
class ReplayBuffer:
    def __init__(self, max_size=10000):
        self.buffer = deque(maxlen=max_size)

    def add(self, experience):
        self.buffer.append(experience)

    def sample(self, batch_size):
        return random.sample(self.buffer, batch_size)

    def size(self):
        return len(self.buffer)

# DQN Agent
class DQNAgent:
    def __init__(self, state_size, action_size, gamma=0.95, epsilon=1.0, epsilon_min=0.01, epsilon_decay=0.995):
        self.state_size = state_size
        self.action_size = action_size
        self.memory = ReplayBuffer()
        self.gamma = gamma
        self.epsilon = epsilon
        self.epsilon_min = epsilon_min
        self.epsilon_decay = epsilon_decay

        self.model = DQNetwork(state_size, action_size)
        self.optimizer = optim.Adam(self.model.parameters(), lr=0.001)
        self.criterion = nn.MSELoss()

    def act(self, state):
        if np.random.rand() <= self.epsilon:
            return random.randrange(self.action_size)
        state = torch.FloatTensor(state).unsqueeze(0)
        q_values = self.model(state)
        return torch.argmax(q_values).item()

    def remember(self, state, action, reward, next_state, done):
        self.memory.add((state, action, reward, next_state, done))

    def replay(self, batch_size=32):
        if self.memory.size() < batch_size:
            return

        batch = self.memory.sample(batch_size)
        for state, action, reward, next_state, done in batch:
            state = torch.FloatTensor(state)
            next_state = torch.FloatTensor(next_state)
            target = self.model(state)[action]
            if done:
                target_val = reward
            else:
                target_val = reward + self.gamma * torch.max(self.model(next_state)).item()

            output = self.model(state)[action]
            loss = self.criterion(output, torch.tensor(target_val))
            self.optimizer.zero_grad()
            loss.backward()
            self.optimizer.step()

        # Decrease epsilon
        if self.epsilon > self.epsilon_min:
            self.epsilon *= self.epsilon_decay



class TradingEnv:
    def __init__(self, data):
        self.data = data
        self.current_step = 0
        self.done = False

    def reset(self):
        self.current_step = 0
        self.done = False
        return self._get_state()

    def _get_state(self):
        return self.data.iloc[self.current_step].values.astype(float)

    def step(self, action):
        # Reward logic (you can customize based on profit/loss logic)
        reward = self._calculate_reward(action)
        self.current_step += 1
        if self.current_step >= len(self.data) - 1:
            self.done = True
        next_state = self._get_state()
        return next_state, reward, self.done

    def _calculate_reward(self, action):
        # Basic example: reward = +1 for 'correct', -1 for 'bad', 0 for No Trade
        # You can replace this with more intelligent logic
        # Let's say column 'action' in data is ground truth
        true_action = int(self.data.iloc[self.current_step]['action'])
        if action == true_action:
            return 1
        elif action == 3:  # No Trade
            return 0
        else:
            return -1


from sklearn.preprocessing import StandardScaler

# Preprocess features
features = ['direction', 'ce_pe_oi_ratio', 'prev_ce_pe_oi_ratio',
            'ce_beta', 'ce_oi_change', 'pre_ce_oi_change', 'first_ce_oi_change',
            'pe_beta', 'pe_oi_change', 'pre_pe_oi_change', 'first_pe_oi_change']

data_df = pd.read_csv("your_data.csv")
data_df = data_df.dropna()  # Clean NaNs
scaler = StandardScaler()
data_df[features] = scaler.fit_transform(data_df[features])

env = TradingEnv(data_df[features + ['action']])  # Include ground truth
agent = DQNAgent(state_size=len(features), action_size=4)  # Buy, Hold, Sell, No Trade

for episode in range(100):
    state = env.reset()
    total_reward = 0
    while not env.done:
        action = agent.act(state)
        next_state, reward, done = env.step(action)
        agent.remember(state, action, reward, next_state, done)
        agent.replay()
        state = next_state
        total_reward += reward
    print(f"Episode {episode + 1}: Total Reward = {total_reward}")
