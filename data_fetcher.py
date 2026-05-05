import yfinance as yf
import pandas as pd
import requests
import io
import time
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
    
    # UPGRADED: 3-Retry loop to prevent silent drops
    def download_single(ticker):
        for attempt in range(3):
            try:
                df = yf.Ticker(ticker).history(period="2y")
                if not df.empty:
                    # 1. Filter out holidays (Phantom rows have 0 volume)
                    df = df[df['Volume'] > 0]
                    # 2. Strip timezones so dates match perfectly
                    df.index = df.index.tz_localize(None)
                    
                    df.columns = pd.MultiIndex.from_product([[ticker], df.columns])
                    return df
            except Exception:
                pass
            time.sleep(1) # Breathe before retrying
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
        print("Step 3: Compiling and saving Stock data...")
        final_data = pd.concat(data_frames, axis=1)
        
        # Save to highly compressed Parquet
        final_data.to_parquet("nifty_750_master.parquet", engine="pyarrow")
        # Save to CSV (as requested in your baseline)
        final_data.to_csv("nifty_750_master.csv")
        print("✅ SUCCESS! nifty_750_master files have been updated.")
    else:
        print("❌ FAILED to download any stock data.")

    # ==========================================================
    # Step 4: FETCH ALL INDICES (BROAD + SECTORAL)
    # ==========================================================
    print("\nStep 4: Fetching 1-Year Historical Data for Indices...")
    broad_indices = ["^NSEI", "^NSMIDCP", "NIFTYMIDCAP150.NS", "HDFCSML250.NS", "^CRSLDX"]
    sectoral_indices = [
        "^CNXSERVICE", "^CNXREALTY", "HDFCPVTBAN.NS", "^CNXPHARMA", "^CNXPSUBANK", 
        "OILIETF.NS", "^CNXMETAL", "^CNXMEDIA", "^CNXMNC", "^CNXINFRA", "^CNXCONSUM", 
        "^CNXIT", "NIFTY_FIN_SERVICE.NS", "^CNXFMCG", "^CNXENERGY", "^CNXCMDT", 
        "CPSEETF.NS", "^NSEBANK", "^CNXAUTO", "MODEFENCE.NS", "MOTOUR.NS", 
        "MOCAPITAL.NS", "AXISHCETF.NS"
    ]
    
    # Combine and remove any duplicates
    all_indices = list(set(broad_indices + sectoral_indices))
    index_frames = []
    
    completed_idx = 0
    for ticker in all_indices:
        for attempt in range(3): # 3-Retry Loop for Safety
            try:
                df = yf.Ticker(ticker).history(period="2y")
                if not df.empty:
                    df = df[['Close']].copy()
                    df.columns = [ticker]
                    df.index = df.index.tz_localize(None) # Align timezones perfectly
                    index_frames.append(df)
                    break
            except Exception:
                pass
            time.sleep(0.5)
            
        completed_idx += 1
        if completed_idx % 5 == 0:
            print(f"   ... Secured {completed_idx} / {len(all_indices)} indices")

    if index_frames:
        print("Step 5: Compiling and saving Index data...")
        merged_idx = pd.concat(index_frames, axis=1)

        # --- NEW: THE MASTER CALENDAR FILTER ---
        # If we successfully downloaded and cleaned stock data...
        if data_frames and 'final_data' in locals():
            # Force the index data to only keep dates that exist in the clean stock data
            merged_idx = merged_idx[merged_idx.index.isin(final_data.index)]
            
        # Fill any internal gaps just in case
        merged_idx = merged_idx.ffill().dropna(how='all')
        
        merged_idx.to_parquet("nifty_indices_master.parquet", engine="pyarrow")
        merged_idx.to_csv("nifty_indices_master.csv")
        print("✅ SUCCESS! nifty_indices_master files have been updated.")
    else:
        print("❌ FAILED to download index data.")
        
    print("\n🚀 FULL SYSTEM REFRESH COMPLETE.")

if __name__ == "__main__":
    update_market_data()
