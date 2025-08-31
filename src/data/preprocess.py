import pandas as pd
import os

def preprocess_data(input_path="data/raw/bitcoin_data.csv", output_path="data/processed/bitcoin_processed.csv"):
    # Créer le dossier processed si nécessaire
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Lire le CSV foiré : sauter les 3 premières lignes, pas d'en-tête, forcer noms de colonnes, ignorer lignes mauvaises
    df = pd.read_csv(input_path, skiprows=3, header=None, names=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'], 
                     on_bad_lines='skip', parse_dates=['Date'], low_memory=False)
    
    # Nettoyer : drop NaN, convertir types numériques
    df = df.dropna()
    df[['Open', 'High', 'Low', 'Close', 'Volume']] = df[['Open', 'High', 'Low', 'Close', 'Volume']].apply(pd.to_numeric, errors='coerce')
    df = df.dropna()  # Re-drop si conversion crée des NaN
    
    # Préparer pour Prophet : 'ds' (date) et 'y' (Close)
    df_prophet = df[['Date', 'Close']].rename(columns={'Date': 'ds', 'Close': 'y'})
    
    # Sauvegarder
    df_prophet.to_csv(output_path, index=False)
    print(f"Données nettoyées et préparées sauvegardées dans {output_path}")
    print(f"Nombre de lignes après nettoyage : {len(df_prophet)}")
    return df_prophet

if __name__ == "__main__":
    preprocess_data()