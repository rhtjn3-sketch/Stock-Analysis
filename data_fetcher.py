import yfinance as yf
import pandas as pd
import requests
import io
import time
import concurrent.futures

def update_market_data():
    print("Step 1: Fetching Nifty Total Market Universe...")
    raw_tickers = []
    industry_map = {}
    total_market_url = "https://www.niftyindices.com/IndexConstituent/ind_niftytotalmarket_list.csv"
    headers = {"User-Agent": "Mozilla/5.0"}

    # Grab the official list and the sector mapping
    try:
        res = requests.get(total_market_url, headers=headers)
        df_raw = pd.read_csv(io.StringIO(res.text))
        df_raw.columns = df_raw.columns.str.strip()
        df_raw['Symbol'] = df_raw['Symbol'].astype(str).str.strip()
        
        if 'Industry' in df_raw.columns:
            df_raw['Industry'] = df_raw['Industry'].astype(str).str.strip()
            industry_map = dict(zip(df_raw['Symbol'], df_raw['Industry']))
            
        raw_tickers = df_raw['Symbol'].dropna().unique().tolist()
    except Exception as e:
        print(f"Error fetching symbols from NSE: {e}")
    
    # Fallback just in case NSE website is down
    if not raw_tickers:
        raw_tickers = ["RELIANCE", "TCS", "HDFCBANK", "ZOMATO"]
        industry_map = {t: "Unknown" for t in raw_tickers}
        
    # Format for Yahoo Finance
    tickers = [ticker if ticker.endswith('.NS') else f"{ticker}.NS" for ticker in raw_tickers]
    total_tickers = len(tickers)
    
    print(f"Step 2: Downloading 2-year history for {total_tickers} stocks via ThreadPool...")
    data_frames = []
    
    # The bulletproof 3-Retry loop for historical data
    def download_single(ticker):
        for attempt in range(3):
            try:
                df = yf.Ticker(ticker).history(period="2y")
                if not df.empty:
                    # Format columns for multi-index compilation later
                    df.columns = pd.MultiIndex.from_product([[ticker], df.columns])
                    return df
            except Exception:
                pass
            time.sleep(1) # Breathe before retrying
        return None

    # Fetch historical data concurrently
    completed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(download_single, t) for t in tickers]
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res is not None:
                data_frames.append(res)
            
            completed += 1
            if completed % 50 == 0:
                print(f"   ... Secured {completed} / {total_tickers} historical records")

    # If we successfully got data, save both files
    if data_frames:
        print("\nStep 3: Compiling Parquet file (Historical Data)...")
        final_data = pd.concat(data_frames, axis=1)
        final_data.to_parquet("nifty_750_master.parquet", engine="pyarrow")
        print("✅ SUCCESS! nifty_750_master.parquet has been updated.")
        
        print("\nStep 4: Fetching Market Caps & Sectors (Metadata)...")
        metadata_list = []
        
        # The 2-Retry loop for live fundamental data
        def fetch_metadata(ticker):
            clean_sym = ticker.replace('.NS', '')
            sector = industry_map.get(clean_sym, 'Unknown')
            mcap_cr = 0.0
            for attempt in range(2):
                try:
                    mcap_raw = yf.Ticker(ticker).fast_info.get('market_cap', 0)
                    if mcap_raw:
                        mcap_cr = round(mcap_raw / 10000000, 2)
                        break
                except Exception:
                    time.sleep(0.5)
            return {"Stock": clean_sym, "Sector": sector, "Market Cap (Cr)": mcap_cr}

        # Fetch metadata concurrently (faster workers because data is lighter)
        completed_meta = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(fetch_metadata, t) for t in tickers]
            for future in concurrent.futures.as_completed(futures):
                metadata_list.append(future.result())
                completed_meta += 1
                if completed_meta % 50 == 0:
                    print(f"   ... Secured {completed_meta} / {total_tickers} fundamental records")
                    
        # Save the metadata CSV
        df_meta = pd.DataFrame(metadata_list)
        df_meta.to_csv("nifty_750_metadata.csv", index=False)
        print("✅ SUCCESS! nifty_750_metadata.csv has been updated.")
        print("\n🚀 FULL SYSTEM REFRESH COMPLETE.")
    else:
        print("❌ FAILED to download any data.")

if __name__ == "__main__":
    update_market_data()
