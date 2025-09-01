import pandas as pd
import os
import numpy as np

def preprocess_data(input_path="data/raw/bitcoin_data.csv", output_path="data/processed/bitcoin_processed.csv"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df = pd.read_csv(input_path, skiprows=3, header=None, names=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'], 
                     on_bad_lines='skip', parse_dates=['Date'], low_memory=False)
    df = df.dropna()
    df[['Open', 'High', 'Low', 'Close', 'Volume']] = df[['Open', 'High', 'Low', 'Close', 'Volume']].apply(pd.to_numeric, errors='coerce')
    df = df.dropna()
    df_prophet = df[['Date', 'Close']].rename(columns={'Date': 'ds', 'Close': 'y'})
    # Appliquer la log-transformation
    df_prophet['y'] = np.log(df_prophet['y'])
    df_prophet.to_csv(output_path, index=False)
    print(f"Données nettoyées et log-transformées sauvegardées dans {output_path}")
    return df_prophet

if __name__ == "__main__":
    preprocess_data()