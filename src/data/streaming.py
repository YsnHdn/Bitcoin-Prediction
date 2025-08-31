import pandas as pd
import redis
import json
import time

def stream_to_redis(csv_path="data/raw/bitcoin_data.csv", redis_host="localhost", redis_port=6379):
    # Lire le CSV avec nettoyage (même que preprocess)
    df = pd.read_csv(csv_path, skiprows=3, header=None, names=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'], 
                     on_bad_lines='skip', parse_dates=['Date'], low_memory=False)
    df = df.dropna()
    df[['Open', 'High', 'Low', 'Close', 'Volume']] = df[['Open', 'High', 'Low', 'Close', 'Volume']].apply(pd.to_numeric, errors='coerce')
    df = df.dropna()
    
    # Connexion Redis
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    
    # Simuler streaming
    for index, row in df.iterrows():
        data = {
            "date": str(row['Date']),
            "open": row["Open"],
            "high": row["High"],
            "low": row["Low"],
            "close": row["Close"],
            "volume": row["Volume"]
        }
        r.xadd("bitcoin_stream", {"data": json.dumps(data)})
        print(f"Envoyé : {data}")
        time.sleep(1)  # Délai simulation

if __name__ == "__main__":
    stream_to_redis()