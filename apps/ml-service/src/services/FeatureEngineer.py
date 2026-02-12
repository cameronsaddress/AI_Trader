import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class FeatureEngineer:
    def __init__(self, window_size=100):
        self.window_size = window_size
        self.data = pd.DataFrame(columns=['timestamp', 'price'])

    def update(self, timestamp, price):
        # Append new data
        new_row = pd.DataFrame({'timestamp': [timestamp], 'price': [float(price)]})
        self.data = pd.concat([self.data, new_row], ignore_index=True)

        # Keep only the window size
        if len(self.data) > self.window_size:
            self.data = self.data.iloc[-self.window_size:].reset_index(drop=True)

        return self.calculate_features()

    def calculate_features(self):
        if len(self.data) < 20:
            return None

        df = self.data.copy()
        
        # Calculate Returns
        df['returns'] = df['price'].pct_change()
        
        # Calculate SMA
        df['sma_20'] = df['price'].rolling(window=20).mean()
        
        # Calculate RSI
        delta = df['price'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi_14'] = 100 - (100 / (1 + rs))

        # Get the latest row
        latest = df.iloc[-1]
        
        features = {
            'price': latest['price'],
            'sma_20': latest['sma_20'],
            'rsi_14': latest['rsi_14'],
            'returns': latest['returns']
        }
        
        # logger.info(f"Features: {features}")
        return features
