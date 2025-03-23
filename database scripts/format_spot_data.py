import pandas as pd
import os
import glob

path = "data/"
all_files = glob.glob(os.path.join(path, "nifty_*.csv"))

nifty_2024_df = pd.DataFrame()

for file in all_files:
    print(file)
    df = pd.read_csv(file)
    date = file.split("_")[1].split(".")[0]

    df = df[df["symbol"] == "NIFTY"]

    df = df.iloc[:, 1:]
    df = df.drop(columns=["oi", "open", "high", "low", "volume", "symbol"])

    df["timestamp"] = pd.to_datetime(date + df["time"], format="%Y%m%d%H:%M:%S")

    df = df.drop(columns=["date", "time"])

    nifty_2024_df = pd.concat([nifty_2024_df, df])

nifty_2024_df = nifty_2024_df.sort_values(by="timestamp")
nifty_2024_df = nifty_2024_df.reset_index(drop=True)

nifty_2024_df.to_parquet("nifty_2024.parquet", index=False)
