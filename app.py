import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import requests
import io

st.set_page_config(page_title="Nifty 750 Pro Engine", layout="wide")

# ==========================================
# PAGE ROUTING SYSTEM (Next/Previous Logic)
# ==========================================
# Initialize the session state for page tracking if it doesn't exist
if 'current_page' not in st.session_state:
    st.session_state.current_page = 1

# Define functions to change pages
def next_page():
    st.session_state.current_page += 1

def prev_page():
    st.session_state.current_page -= 1

# ==========================================
# DATA LOADING ENGINE
# ==========================================
@st.cache_data 
def load_data():
    raw_tickers = []
    
    # 1. Fetch live CSV directly from the new Nifty Total Market URL
    #total_market_url = "https://www.niftyindices.com/IndexConstituent/ind_niftytotalmarket_list.csv"
    total_market_url = "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap50list.csv"
    
    # We use a User-Agent header so the NSE website doesn't block our script
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        with st.spinner('Fetching latest Total Market constituents from NiftyIndices.com...'):
            res = requests.get(total_market_url, headers=headers)
            df_raw = pd.read_csv(io.StringIO(res.text))
            
            # Clean column names just in case NSE adds trailing spaces
            df_raw.columns = df_raw.columns.str.strip()
            # Clean the string values to ensure perfect matching
            df_raw['Symbol'] = df_raw['Symbol'].astype(str).str.strip()
            
            # Map the Industry directly from the NSE file
            if 'Industry' in df_raw.columns:
                df_raw['Industry'] = df_raw['Industry'].astype(str).str.strip()
                industry_map = dict(zip(df_raw['Symbol'], df_raw['Industry']))                                                    
            raw_tickers = df_raw['Symbol'].dropna().unique().tolist()
            
    except Exception as e:
        st.error(f"Could not fetch live lists: {e}.")
    
    # Fallback just in case the URL fails
    if not raw_tickers:
        st.warning("⚠️ Using fallback sample list.")
        raw_tickers = ["RELIANCE", "TCS", "HDFCBANK", "ZOMATO", "SUZLON", "PAYTM"]
        industry_map = {t: "Unknown" for t in raw_tickers}                                                  
    # Safely append .NS, ensuring we don't duplicate the suffix if it somehow exists
    tickers = [ticker if ticker.endswith('.NS') else f"{ticker}.NS" for ticker in raw_tickers]
    total_tickers = len(tickers)
    
    # 2. Download Historical Price Data (Bulk = Fast)
    with st.spinner(f'Downloading price data for {total_tickers} stocks...'):
        data = yf.download(tickers, period="1y", group_by='ticker', threads=True)
    
    # 3. Setup Progress Bar for Market Cap & Sector fetching
    progress_text = "Calculating metrics & Market Cap. Please wait..."
    my_bar = st.progress(0, text=progress_text)
    
    metrics = []
    
    for i, ticker in enumerate(tickers):
        my_bar.progress((i + 1) / total_tickers, text=f"Processing {i+1}/{total_tickers}: {ticker}")
        
        try:
            if ticker in data:
                df = data[ticker].dropna()
            else:
                df = data.dropna()
                
            if df.empty: continue
            
            clean_symbol = ticker.replace('.NS', '')                                       
            # Fetch Fundamentals (Market Cap AND Sector)
            stock_info = yf.Ticker(ticker).info
            market_cap_raw = stock_info.get('marketCap')
            
            # Pull Sector/Industry directly from our local dictionary, bypassing Yahoo
            sector = industry_map.get(clean_symbol, 'Unknown')
            
            # Safe Market Cap logic
            if market_cap_raw is None or market_cap_raw == 0:
                market_cap_cr = 0.0
            else:
                market_cap_cr = round(market_cap_raw / 10000000, 2)
            
            current_price = df['Close'].iloc[-1]
            
            # STRICT checks for trading days to prevent false returns for new IPOs
            ret_1w = (current_price / df['Close'].iloc[-6] - 1) * 100 if len(df) >= 6 else np.nan
            ret_1m = (current_price / df['Close'].iloc[-22] - 1) * 100 if len(df) >= 22 else np.nan
            ret_3m = (current_price / df['Close'].iloc[-64] - 1) * 100 if len(df) >= 64 else np.nan
            ret_6m = (current_price / df['Close'].iloc[-126] - 1) * 100 if len(df) >= 126 else np.nan
            ret_1y = (current_price / df['Close'].iloc[-250] - 1) * 100 if len(df) >= 250 else np.nan
            
            # Calculate Moving Averages
            sma_50 = df['Close'].rolling(window=50).mean().iloc[-1] if len(df) >= 50 else np.nan
            sma_200 = df['Close'].rolling(window=200).mean().iloc[-1] if len(df) >= 200 else np.nan
             
            metrics.append({
                "Stock": clean_symbol,
                "Sector": sector,
                "Market Cap (Cr)": market_cap_cr,
                "Price": round(current_price, 2),
                "1W Return (%)": round(ret_1w, 2) if not np.isnan(ret_1w) else None,
                "1M Return (%)": round(ret_1m, 2) if not np.isnan(ret_1m) else None,
                "3M Return (%)": round(ret_3m, 2) if not np.isnan(ret_3m) else None,
                "6M Return (%)": round(ret_6m, 2) if not np.isnan(ret_6m) else None,
                "1Y Return (%)": round(ret_1y, 2) if not np.isnan(ret_1y) else None,
                "Above 50 DMA?": "Yes" if not np.isnan(sma_50) and current_price > sma_50 else "No",
                "Above 200 DMA?": "Yes" if not np.isnan(sma_200) and current_price > sma_200 else "No"
            })
        except Exception as e:
            pass
            
    my_bar.empty()
    return pd.DataFrame(metrics)

# Load the core dataset
df = load_data()

# ==========================================
# UI: TOP NAVIGATION BAR
# ==========================================
col_prev, col_title, col_next = st.columns([1, 8, 1])

with col_prev:
    # Only show 'Previous' if we are not on the first page
    if st.session_state.current_page > 1:
        st.button("⬅️ Previous", on_click=prev_page)

with col_title:
    # Dynamically change the main title based on the page
    if st.session_state.current_page == 1:
        st.markdown("<h2 style='text-align: center;'>Part 1: Market Watchlist</h2>", unsafe_allow_html=True)
    elif st.session_state.current_page == 2:
        st.markdown("<h2 style='text-align: center;'>Part 2: Sector & Breadth Analysis</h2>", unsafe_allow_html=True)

with col_next:
    # Only show 'Next' if we have more pages (Currently max 2 pages)
    if st.session_state.current_page < 2:
        st.button("Next ➡️", on_click=next_page)

st.divider() # A clean visual line separating nav from content

# ==========================================
# PAGE 1: MARKET WATCHLIST
# ==========================================
if st.session_state.current_page == 1:
    
    # 1. The Interactive UI Controls for Page 1
    st.sidebar.header("Filter & Rank Engine")
    
    # Included Sector in sorting options
    sort_options = ["Market Cap (Cr)", "1M Return (%)", "1W Return (%)", "3M Return (%)", "6M Return (%)", "1Y Return (%)", "Price", "Sector"]
    sort_by = st.sidebar.selectbox("Rank Stocks By:", sort_options)
    sort_order = st.sidebar.radio("Order:", ["Descending (Top Performers/Largest)", "Ascending (Bottom Performers/Smallest)"])
    
    # Sector Filter
    all_sectors = ["All"] + sorted(list(df["Sector"].unique()))
    selected_sector = st.sidebar.selectbox("Filter by Sector:", all_sectors)
    
    min_mcap = st.sidebar.number_input("Minimum Market Cap (in Crores)", min_value=0, value=0, step=500)
    filter_dma_50 = st.sidebar.checkbox("Only show stocks Above 50 DMA")
    filter_dma_200 = st.sidebar.checkbox("Only show stocks Above 200 DMA")

    # 2. Apply the Logic
    ascending = True if sort_order.startswith("Ascending") else False
    
    # Create a working copy of the dataframe to filter
    df_filtered = df.copy()
    
    # Apply Sector filter FIRST to establish the "relevant scenario" base denominator
    if selected_sector != "All":
        df_filtered = df_filtered[df_filtered["Sector"] == selected_sector]
        
    # Capture the total count for the selected sector(s) before applying technical filters
    base_total = len(df_filtered)
        
    # Now apply the technical and size filters
    df_filtered = df_filtered[df_filtered["Market Cap (Cr)"] >= min_mcap]
    if filter_dma_50: df_filtered = df_filtered[df_filtered["Above 50 DMA?"] == "Yes"]
    if filter_dma_200: df_filtered = df_filtered[df_filtered["Above 200 DMA?"] == "Yes"]
        
    df_sorted = df_filtered.sort_values(by=sort_by, ascending=ascending).reset_index(drop=True)

    # Shift the index to start from 1 instead of 0
    df_sorted.index = df_sorted.index + 1

    # Calculate final filtered counts and percentages
    filtered_total = len(df_sorted)
    percentage = (filtered_total / base_total * 100) if base_total > 0 else 0

    # 3. Display the Results
    
    # Display the dynamic count and percentage right-aligned above the table
    st.markdown(
        f"<div style='text-align: right; font-size: 16px; font-weight: bold; padding-bottom: 10px;'>"
        f"Matches: <span style='color: #4CAF50;'>{filtered_total}</span> / {base_total} "
        f"(<span style='color: #4CAF50;'>{percentage:.1f}%</span>)</div>", 
        unsafe_allow_html=True
    )

    # Display the dataframe using full width
    st.dataframe(df_sorted.style.format(
        formatter={
            "Market Cap (Cr)": "{:,.2f}",
            "1W Return (%)": "{:.2f}%",
            "1M Return (%)": "{:.2f}%",
            "3M Return (%)": "{:.2f}%",
            "6M Return (%)": "{:.2f}%",
            "1Y Return (%)": "{:.2f}%",
        },
        na_rep="-"  # Tells pandas to print a dash "-" instead of crashing on empty data
    ), use_container_width=True, height=600)

# ==========================================
# PAGE 2: PLACEHOLDER FOR NEXT FEATURES
# ==========================================
elif st.session_state.current_page == 2:
    st.write("This is where we will build the next visualizations (like the Sectoral Momentum heatmap or Global Indices).")
    st.info("Let me know which screenshot you want to tackle next!")
