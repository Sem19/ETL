from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

def log_progress(message):
    ''' Logs a timestamped message to code_log.txt '''
    timestamp_format = "%Y-%m-%d %H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a") as f:
        f.write(f"{timestamp} : {message}\n")

def extract(url):
    ''' Extracts bank name and market cap from the target Wikipedia table '''
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    tables = soup.find_all("table", {"class": "wikitable"})
    target_table = tables[0] 
    rows = target_table.find_all("tr")

    data = []
    for row in rows[1:]:
        cols = row.find_all("td")
        if len(cols) >= 3:
            bank_name = cols[1].get_text(strip=True)
            market_cap_raw = cols[2].get_text(strip=True).replace('\n', '').replace(",", '')
            try:
                market_cap = float(market_cap_raw)
                data.append({
                    "Bank Name": bank_name,
                    "Market Cap (US$ Billion)": market_cap
                })
            except ValueError:
                continue  
    df = pd.DataFrame(data)
    return df

def transform(df, exchange_rate_csv):
    ''' Reads exchange rates and adds converted columns rounded to 2 decimals '''
    exchange_df = pd.read_csv(exchange_rate_csv)
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']

    df.rename(columns={"Market Cap (US$ Billion)": "MC_USD_Billion"}, inplace=True)

    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    df.rename(columns={"Bank Name": "Name"}, inplace=True)

    return df

def load_to_csv(df, path):
    df.to_csv(path, index=False)

def load_to_db(df, connection, table_name):
    df.to_sql(table_name, connection, if_exists="replace", index=False)

def run_query(query, connection):
    print(f"Executing query:\n{query}\n")
    result = pd.read_sql(query, connection)
    print(result)

# ------------------- ETL Pipeline -------------------

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path = "./Largest_Banks.csv"
banks_db = "Banks.db"
banks_table = "Largest_banks"
exchange_rates_file = "exchange_rate.csv"

log_progress("Preliminaries complete. Initiating ETL process")

# Step 1: Extract
df = extract(url)
log_progress("Data extraction complete.")

# Step 2: Transform
df = transform(df, exchange_rates_file)
log_progress("Data transformation complete.")

# Вивід після трансформації
print(df.head())

# Step 3: Load
load_to_csv(df, csv_path)
log_progress("Data saved to CSV file")

conn = sqlite3.connect(banks_db)
log_progress("SQL Connection initiated")

load_to_db(df, conn, banks_table)
log_progress("Data loaded to Database as a table, Executing queries")

# Приклади запитів
query_all = f"""
SELECT Name, MC_USD_Billion, MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion
FROM {banks_table}
LIMIT 10
"""
run_query(query_all, conn)

query_avg = f"""
SELECT AVG(MC_GBP_Billion) as Avg_GBP_Market_Cap

FROM {banks_table}
"""
run_query(query_avg, conn)

query_5_lagest = f"""
SELECT Name
FROM {banks_table}
LIMIT 5
"""
run_query(query_5_lagest, conn)


conn.close()
log_progress("Server Connection closed")
log_progress("Process Complete")
