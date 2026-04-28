import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import requests
import io

st.set_page_config(page_title="Nifty 750 Screener", layout="wide")

st.title("📈 Live Nifty 750 Dynamic Ranker")
st.markdown("This dashboard pulls live constituent lists and market data on the fly.")

@st.cache_data 
def load_data():
    raw_tickers = []
    
    # 1. Fetch live CSVs directly from NSE URLs
    nifty500_url = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"
    microcap250_url = "https://www.niftyindices.com/IndexConstituent/ind_niftymicrocap250_list.csv"
    
    # We use a User-Agent header so the NSE website doesn't block our script as a bot
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        with st.spinner('Fetching latest 750 constituents from NiftyIndices.com...'):
            res_500 = requests.get(nifty500_url, headers=headers)
            df_500 = pd.read_csv(io.StringIO(res_500.text))
            
            res_250 = requests.get(microcap250_url, headers=headers)
            df_250 = pd.read_csv(io.StringIO(res_250.text))
            
            raw_tickers = pd.concat([df_500['Symbol'], df_250['Symbol']]).dropna().unique().tolist()
    except Exception as e:
        st.error(f"Could not fetch live lists: {e}. Ensure you have internet access or the NSE site isn't blocking the request.")
    
    # Fallback just in case the URLs fail
    if not raw_tickers:
        st.warning("⚠️ Using fallback sample list.")
        raw_tickers = ["RELIANCE", "TCS", "HDFCBANK", "ZOMATO", "SUZLON", "PAYTM"]
        
    tickers = [f"{ticker}.NS" for ticker in raw_tickers]
    total_tickers = len(tickers)
    
    # 2. Download Historical Price Data (Bulk = Fast)
    with st.spinner(f'Downloading price data for {total_tickers} stocks...'):
        data = yf.download(tickers, period="1y", group_by='ticker', threads=True)
    
    # 3. Setup Progress Bar for Market Cap fetching
    progress_text = "Calculating metrics and fetching Market Cap. Please wait..."
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
            
            # Fetch Market Cap
            stock_info = yf.Ticker(ticker).info
            market_cap_raw = stock_info.get('marketCap', 0)
            market_cap_cr = round(market_cap_raw / 10000000, 2) if market_cap_raw else 0
            
            current_price = df['Close'].iloc[-1]
            
            ret_1w = (current_price / df['Close'].iloc[-6] - 1) * 100 if len(df) >= 6 else np.nan
            ret_1m = (current_price / df['Close'].iloc[-22] - 1) * 100 if len(df) >= 22 else np.nan
            ret_3m = (current_price / df['Close'].iloc[-64] - 1) * 100 if len(df) >= 64 else np.nan
            ret_6m = (current_price / df['Close'].iloc[-126] - 1) * 100 if len(df) >= 126 else np.nan
            ret_1y = (current_price / df['Close'].iloc[0] - 1) * 100 if len(df) > 0 else np.nan
            
            sma_50 = df['Close'].rolling(window=50).mean().iloc[-1]
            sma_200 = df['Close'].rolling(window=200).mean().iloc[-1]
             
            metrics.append({
                "Stock": ticker.replace('.NS', ''),
                "Market Cap (Cr)": market_cap_cr,
                "Price": round(current_price, 2),
                "1W Return (%)": round(ret_1w, 2),
                "1M Return (%)": round(ret_1m, 2),
                "3M Return (%)": round(ret_3m, 2),
                "6M Return (%)": round(ret_6m, 2),
                "1Y Return (%)": round(ret_1y, 2),
                "Above 50 DMA?": "Yes" if current_price > sma_50 else "No",
                "Above 200 DMA?": "Yes" if current_price > sma_200 else "No"
            })
        except Exception as e:
            pass
            
    my_bar.empty()
    return pd.DataFrame(metrics)

# Load the data
df = load_data()

# 2. The Interactive UI Controls
st.sidebar.header("Filter & Rank Engine")

sort_options = ["Market Cap (Cr)", "1M Return (%)", "1W Return (%)", "3M Return (%)", "6M Return (%)", "1Y Return (%)", "Price"]
sort_by = st.sidebar.selectbox("Rank Stocks By:", sort_options)
sort_order = st.sidebar.radio("Order:", ["Descending (Top Performers)", "Ascending (Bottom Performers)"])
min_mcap = st.sidebar.number_input("Minimum Market Cap (in Crores)", min_value=0, value=0, step=500)
filter_dma_50 = st.sidebar.checkbox("Only show stocks Above 50 DMA")
filter_dma_200 = st.sidebar.checkbox("Only show stocks Above 200 DMA")

# 3. Apply the Logic
ascending = True if sort_order.startswith("Ascending") else False
df = df[df["Market Cap (Cr)"] >= min_mcap]

if filter_dma_50: df = df[df["Above 50 DMA?"] == "Yes"]
if filter_dma_200: df = df[df["Above 200 DMA?"] == "Yes"]
    
df_sorted = df.sort_values(by=sort_by, ascending=ascending).reset_index(drop=True)

# 4. Display the Results
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader(f"Ranked by {sort_by}")
    st.dataframe(df_sorted.style.format({
        "Market Cap (Cr)": "{:,.2f}",
        "1W Return (%)": "{:.2f}%",
        "1M Return (%)": "{:.2f}%",
        "3M Return (%)": "{:.2f}%",
        "6M Return (%)": "{:.2f}%",
        "1Y Return (%)": "{:.2f}%",
    }), use_container_width=True)

with col2:
    st.subheader("Top 5 Chart")
    top_5 = df_sorted.head(5)
    st.bar_chart(data=top_5, x="Stock", y=sort_by)