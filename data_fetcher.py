import yfinance as yf
import pandas as pd
import requests
import io
import concurrent.futures

def update_market_data():
    print("Step 1: Fetching Nifty Total Market Universe...")
    raw_tickers = []
    total_market_url = "https://www.niftyindices.com/IndexConstituent/ind_niftytotalmarket_list.csv"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        res = requests.get(total_market_url, headers=headers)
        df_raw = pd.read_csv(io.StringIO(res.text))
        df_raw.columns = df_raw.columns.str.strip()
        df_raw['Symbol'] = df_raw['Symbol'].astype(str).str.strip()
        raw_tickers = df_raw['Symbol'].dropna().unique().tolist()
    except Exception as e:
        print(f"Error fetching symbols: {e}")
    
    if not raw_tickers:
        raw_tickers = ["RELIANCE", "TCS", "HDFCBANK", "ZOMATO"]
        
    tickers = [ticker if ticker.endswith('.NS') else f"{ticker}.NS" for ticker in raw_tickers]
    total_tickers = len(tickers)
    
    print(f"Step 2: Downloading 2-year history for {total_tickers} stocks via ThreadPool...")
    data_frames = []
    
    def download_single(ticker):
        try:
            df = yf.Ticker(ticker).history(period="2y")
            if not df.empty:
                df.columns = pd.MultiIndex.from_product([[ticker], df.columns])
                return df
        except Exception:
            pass
        return None

    # Using ThreadPool to bypass rate limits cleanly
    completed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(download_single, t) for t in tickers]
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res is not None:
                data_frames.append(res)
            
            completed += 1
            if completed % 50 == 0:
                print(f"   ... Secured {completed} / {total_tickers} stocks")

    if data_frames:
        print("Step 3: Compiling and saving to Parquet file...")
        final_data = pd.concat(data_frames, axis=1)
        
        # Save to a highly compressed file format
        final_data.to_parquet("nifty_750_master.parquet", engine="pyarrow")
        # Save to CSV instead
        final_data.to_csv("nifty_750_master.csv")
        print("✅ SUCCESS! nifty_750_master.parquet has been updated.")
    else:
        print("❌ FAILED to download any data.")

if __name__ == "__main__":
    update_market_data()
