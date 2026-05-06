import yfinance as yf
import pandas as pd
import requests
import io
import time
import concurrent.futures
import os
import datetime

# ==========================================
# INCREMENTAL UPDATE LOGIC (OVERLAP UPGRADE)
# ==========================================
def get_missing_data(ticker, last_date):
    """Fetches a 5-day overlapping window to overwrite mid-day incomplete data."""
    for attempt in range(3):
        try:
            # THE OVERLAP RULE: Go back 5 days to capture final closing corrections
            start_fetch_date = last_date - datetime.timedelta(days=5)
            
            df = yf.Ticker(ticker).history(start=start_fetch_date.strftime('%Y-%m-%d'))
            
            if not df.empty:
                if 'Volume' in df.columns:
                    df = df[df['Volume'] > 0] # Filter holidays
                
                # .normalize() ensures timestamp is exactly 00:00:00 for perfect overwriting
                df.index = df.index.tz_localize(None).normalize() 
                return df
        except Exception:
            pass
        time.sleep(1)
    return pd.DataFrame()

def download_full_history(ticker, period="2y"):
    """Fetches the full 2-year history (Used only on initial setup)"""
    for attempt in range(3):
        try:
            df = yf.Ticker(ticker).history(period=period)
            if not df.empty:
                if 'Volume' in df.columns:
                    df = df[df['Volume'] > 0]
                # .normalize() ensures perfect timestamp alignment
                df.index = df.index.tz_localize(None).normalize()
                return df
        except Exception:
            pass
        time.sleep(1) 
    return pd.DataFrame()

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
    
    # ==========================================
    # STOCKS: SMART SYNC
    # ==========================================
    print(f"\nStep 2: Checking existing Stock Database...")
    master_file = "nifty_750_master.parquet"
    
    if os.path.exists(master_file):
        print("   Found existing database! Running Incremental Sync...")
        existing_data = pd.read_parquet(master_file)
        existing_data.index = pd.to_datetime(existing_data.index)
        last_recorded_date = existing_data.index.max()
        print(f"   Last recorded date is: {last_recorded_date.strftime('%Y-%m-%d')}")
        
        new_data_frames = []
        completed = 0
        
        def process_incremental(t):
            df = get_missing_data(t, last_recorded_date)
            if not df.empty:
                df.columns = pd.MultiIndex.from_product([[t], df.columns])
                return df
            return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_incremental, t) for t in tickers]
            for future in concurrent.futures.as_completed(futures):
                res = future.result()
                if res is not None:
                    new_data_frames.append(res)
                completed += 1
                if completed % 100 == 0:
                    print(f"   ... Checked {completed} / {total_tickers} stocks")
                    
        if new_data_frames:
            print("   Merging new data with existing database...")
            new_data = pd.concat(new_data_frames, axis=1)
            final_data = pd.concat([existing_data, new_data])
            final_data = final_data[~final_data.index.duplicated(keep='last')]
        else:
            print("   Database is already up to date! No new data found.")
            final_data = existing_data

    else:
        print("   No existing database found. Running Full 2-Year Download...")
        data_frames = []
        completed = 0
        
        def process_full(t):
            df = download_full_history(t, "2y")
            if not df.empty:
                df.columns = pd.MultiIndex.from_product([[t], df.columns])
                return df
            return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_full, t) for t in tickers]
            for future in concurrent.futures.as_completed(futures):
                res = future.result()
                if res is not None:
                    data_frames.append(res)
                completed += 1
                if completed % 50 == 0:
                    print(f"   ... Secured {completed} / {total_tickers} stocks")
                    
        if data_frames:
            final_data = pd.concat(data_frames, axis=1)
        else:
            print("❌ FAILED to download any stock data.")
            return

    # Save the updated stock database
    print("Step 3: Saving Stock data...")
    final_data.to_parquet(master_file, engine="pyarrow")
    print(f"✅ SUCCESS! {master_file} has been updated.")


    # ==========================================================
    # INDICES: SMART SYNC
    # ==========================================================
    print("\nStep 4: Checking existing Indices Database...")
    broad_indices = ["^NSEI", "^NSMIDCP", "NIFTYMIDCAP150.NS", "HDFCSML250.NS", "^CRSLDX"]
    sectoral_indices = [
        "^CNXSERVICE", "^CNXREALTY", "HDFCPVTBAN.NS", "^CNXPHARMA", "^CNXPSUBANK", 
        "OILIETF.NS", "^CNXMETAL", "^CNXMEDIA", "^CNXMNC", "^CNXINFRA", "^CNXCONSUM", 
        "^CNXIT", "NIFTY_FIN_SERVICE.NS", "^CNXFMCG", "^CNXENERGY", "^CNXCMDT", 
        "CPSEETF.NS", "^NSEBANK", "^CNXAUTO", "MODEFENCE.NS", "MOTOUR.NS", 
        "MOCAPITAL.NS", "AXISHCETF.NS"
    ]
    all_indices = list(set(broad_indices + sectoral_indices))
    idx_file = "nifty_indices_master.parquet"
    
    if os.path.exists(idx_file):
        print("   Found existing Indices database! Running Incremental Sync...")
        existing_idx = pd.read_parquet(idx_file)
        existing_idx.index = pd.to_datetime(existing_idx.index)
        last_idx_date = existing_idx.index.max()
        
        new_idx_frames = []
        for ticker in all_indices:
            df = get_missing_data(ticker, last_idx_date)
            if not df.empty:
                df = df[['Close']].copy()
                df.columns = [ticker]
                new_idx_frames.append(df)
            time.sleep(0.2)
            
        if new_idx_frames:
            new_idx = pd.concat(new_idx_frames, axis=1)
            merged_idx = pd.concat([existing_idx, new_idx])
            merged_idx = merged_idx[~merged_idx.index.duplicated(keep='last')]
        else:
            merged_idx = existing_idx
    else:
        print("   No existing database found. Running Full 1-Year Download...")
        index_frames = []
        for ticker in all_indices:
            df = download_full_history(ticker, "1y")
            if not df.empty:
                df = df[['Close']].copy()
                df.columns = [ticker]
                index_frames.append(df)
            time.sleep(0.5)
            
        if index_frames:
            merged_idx = pd.concat(index_frames, axis=1)
        else:
            merged_idx = pd.DataFrame()

    if not merged_idx.empty:
        # Master Calendar Filter
        if 'final_data' in locals() and not final_data.empty:
            merged_idx = merged_idx[merged_idx.index.isin(final_data.index)]
        merged_idx = merged_idx.ffill().dropna(how='all')
        merged_idx.to_parquet(idx_file, engine="pyarrow")
        print("✅ SUCCESS! nifty_indices_master files updated.")

    # ==========================================================
    # WORLD & MACRO: SMART SYNC
    # ==========================================================
    print("\nStep 5: Checking World Indices & Macro Assets...")
    world_tickers = ["^KS11", "^N225", "^BVSP", "^DJI", "^FTSE", "^RUT", "NQ=F", "^GSPC", "^BSESN", "^FCHI", "^GDAXI", "^HSI", "^MXX", "^STOXX50E", "^STI", "^TWII", "^AXJO", "^GSPTSE"]
    macro_tickers = ["DX-Y.NYB", "GC=F", "CL=F", "^TNX", "INR=X", "^NSEI"] 
    
    def sync_global_file(filename, ticker_list):
        if os.path.exists(filename):
            print(f"   Syncing {filename}...")
            exist_df = pd.read_parquet(filename)
            exist_df.index = pd.to_datetime(exist_df.index)
            last_date = exist_df.index.max()
            
            new_frames = []
            for t in ticker_list:
                df = get_missing_data(t, last_date)
                if not df.empty:
                    df = df[['Close']].copy()
                    df.columns = [t]
                    new_frames.append(df)
                time.sleep(0.2)
                
            if new_frames:
                new_df = pd.concat(new_frames, axis=1)
                merged_df = pd.concat([exist_df, new_df])
                merged_df = merged_df[~merged_df.index.duplicated(keep='last')]
                return merged_df
            return exist_df
        else:
            print(f"   Full Download for {filename}...")
            frames = []
            for t in ticker_list:
                df = download_full_history(t, "1y")
                if not df.empty:
                    df = df[['Close']].copy()
                    df.columns = [t]
                    frames.append(df)
                time.sleep(0.5)
            if frames:
                return pd.concat(frames, axis=1)
            return pd.DataFrame()

    world_df = sync_global_file("world_indices_master.parquet", world_tickers)
    if not world_df.empty:
        world_df = world_df.ffill().dropna(how='all')
        world_df.to_parquet("world_indices_master.parquet", engine="pyarrow")

    macro_df = sync_global_file("macro_master.parquet", macro_tickers)
    if not macro_df.empty:
        macro_df = macro_df.ffill().dropna(how='all')
        macro_df.to_parquet("macro_master.parquet", engine="pyarrow")
        
    print("✅ SUCCESS! Global & Macro master files updated.")
    print("\n🚀 FULL SYSTEM REFRESH COMPLETE.")

if __name__ == "__main__":
    update_market_data()
