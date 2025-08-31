import yfinance as yf
import pandas as pd
import os

def fetch_bitcoin_data(start_date="2023-01-01", end_date="2025-08-31", output_path="data/raw/bitcoin_data.csv"):
    # Créer le dossier si nécessaire
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Télécharger données fraîches
    btc_data = yf.download("BTC-USD", start=start_date, end=end_date, interval="1d")
    
    # Colonnes standard
    btc_data = btc_data[["Open", "High", "Low", "Close", "Volume"]].reset_index()
    btc_data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']  # Force les noms
    
    # Sauvegarder sans en-têtes inutiles
    btc_data.to_csv(output_path, index=False)
    print(f"Données fraîches sauvegardées dans {output_path}")
    return btc_data

if __name__ == "__main__":
    fetch_bitcoin_data()