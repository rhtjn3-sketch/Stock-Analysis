import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import requests
import io
import plotly.express as px  

st.set_page_config(page_title="Nifty 750 Pro Engine", layout="wide")

# ==========================================
# PAGE ROUTING SYSTEM (Next/Previous Logic)
# ==========================================
# Initialize the session state for page tracking if it doesn't exist
if 'current_page' not in st.session_state:
    st.session_state.current_page = 1

# Define functions to change pages
def next_page():
    if st.session_state.current_page < 3:
        st.session_state.current_page += 1

def prev_page():
    if st.session_state.current_page > 1:
        st.session_state.current_page -= 1

# =======================================================
# DATA ENGINE 1: Fetching & Filtering 750 Stocks (for Watchlist)
# =======================================================
@st.cache_data 
def load_data_watchlist():
    raw_tickers = []
    industry_map = {}
    
    # 1. Fetch live CSV directly from the official Nifty Total Market URL
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
            
            # Map the Industry directly from the NSE file for reliability
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
        data = yf.download(tickers, period="2y", group_by='ticker', threads=True)
    
    # 3. Setup Progress Bar for Market Cap fetching
    progress_text = "Calculating metrics & Market Cap. Please wait..."
    my_bar = st.progress(0, text=progress_text)
    
    metrics = []
    failed_tickers = []
    
    for i, ticker in enumerate(tickers):
        my_bar.progress((i + 1) / total_tickers, text=f"Processing {i+1}/{total_tickers}: {ticker}")
        
        try:
            if ticker in data:
                df = data[ticker].dropna()
            else:
                df = data.dropna()
                
            if df.empty: continue
            
            clean_symbol = ticker.replace('.NS', '')                                       
            # Fetch Fundamentals (Market Cap)
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
    # If any stocks failed, show a warning on the screen
    if failed_tickers:
        st.warning(f"⚠️ Could not fetch data for {len(failed_tickers)} stocks: {', '.join(failed_tickers)}")
    return pd.DataFrame(metrics)

# =======================================================
# DATA ENGINE 2: Fetching Index Performance & Time Series History
# =======================================================
@st.cache_data
def load_index_data(index_list_with_names):
    """
    Dedicated bulk loader to fetch historical performance for indices.
    NOW RETURNS: A tuple of (Summary Metrics DataFrame, Daily History DataFrame)
    """
    symbol_to_name = {ticker: name for ticker, name in index_list_with_names.items()}
    tickers = list(symbol_to_name.keys())
    total_indices = len(tickers)
    
    with st.spinner(f'Downloading performance data for {total_indices} market indices...'):
        data = yf.download(tickers, period="1y", group_by='ticker', threads=True)
    
    results = []
    history_dict = {} # Dictionary to store daily closing prices
    
    for i, ticker in enumerate(tickers):
        try:
            df = data[ticker].dropna() if len(tickers) > 1 else data
            if df.empty: continue
            
            index_name = symbol_to_name.get(ticker, ticker)
            
            # Store the close history for our new waveform chart
            history_dict[index_name] = df['Close']
            
            current_price = df['Close'].iloc[-1]
            
            ret_1w = (current_price / df['Close'].iloc[-6] - 1) * 100 if len(df) >= 6 else np.nan
            ret_1m = (current_price / df['Close'].iloc[-22] - 1) * 100 if len(df) >= 22 else np.nan
            ret_3m = (current_price / df['Close'].iloc[-64] - 1) * 100 if len(df) >= 64 else np.nan
            ret_6m = (current_price / df['Close'].iloc[-126] - 1) * 100 if len(df) >= 126 else np.nan
            ret_1y = (current_price / df['Close'].iloc[0] - 1) * 100 if len(df) > 0 else np.nan
            
            results.append({
                "Index Ticker": ticker,
                "Market Index": index_name,
                "Price": round(current_price, 2),
                "1W Return (%)": round(ret_1w, 2),
                "1M Return (%)": round(ret_1m, 2),
                "3M Return (%)": round(ret_3m, 2),
                "6M Return (%)": round(ret_6m, 2),
                "1Y Return (%)": round(ret_1y, 2),
            })
        except Exception as e:
            pass
            
    df_metrics = pd.DataFrame(results)
    df_history = pd.DataFrame(history_dict) # Build the history dataframe
    
    return df_metrics, df_history

# =======================================================
# DATA ENGINE 3: Fetching Specific Sector Constituents from NSE
# =======================================================
@st.cache_data
def fetch_sector_constituents(sector_name):
    """
    Downloads the official CSV from NSE for a specific sector and returns the list of stock symbols.
    """
    # Mapping our display names to their official NSE CSV filenames
    url_map = {
        "Auto": "ind_niftyautolist.csv",
        "Bank": "ind_niftybanklist.csv",
        "IT": "ind_niftyitlist.csv",
        "Pharma": "ind_niftypharmalist.csv",
        "Realty": "ind_niftyrealtylist.csv",
        "FMCG": "ind_niftyfmcglist.csv",
        "Metal": "ind_niftymetallist.csv",
        "Energy": "ind_niftyenergylist.csv",
        "Infra": "ind_niftyinfralist.csv",
        "MNC": "ind_niftymnclist.csv",
        "PSU Bank": "ind_niftypsubanklist.csv",
        "Private Bank": "ind_niftyprivatebanklist.csv",
        "Media": "ind_niftymedialist.csv",
        "Fin Serv": "ind_niftyfinlist.csv",
        "Commodities": "ind_niftycommoditieslist.csv",
        "Consumption": "ind_niftyconsumptionlist.csv",
        "CPSE": "ind_niftycpselist.csv",
        "Service": "ind_niftyservicelist.csv"
    }
    
    file_name = url_map.get(sector_name)
    if not file_name:
        return [] # Return empty list if we don't have a direct mapping
        
    url = f"https://www.niftyindices.com/IndexConstituent/{file_name}"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        res = requests.get(url, headers=headers)
        df = pd.read_csv(io.StringIO(res.text))
        # Clean column names and extract the Symbol column
        df.columns = df.columns.str.strip()
        symbols = df['Symbol'].astype(str).str.strip().tolist()
        return symbols
    except Exception as e:
        return []

# ==========================================
# UI: TOP NAVIGATION BAR
# ==========================================
st.markdown("<br>", unsafe_allow_html=True) 
col_prev, col_title, col_next = st.columns([1, 8, 1])

with col_prev:
    if st.session_state.current_page > 1:
        st.button("⬅️ Previous Screen", on_click=prev_page)

with col_title:
    if st.session_state.current_page == 1:
        st.markdown("<h2 style='text-align: center;'>Part 1: Market Watchlist</h2>", unsafe_allow_html=True)
    elif st.session_state.current_page == 2:
        st.markdown("<h2 style='text-align: center;'>Part 2: Nifty Broad Indices Benchmarking</h2>", unsafe_allow_html=True)
    elif st.session_state.current_page == 3:
        st.markdown("<h2 style='text-align: center;'>Part 3: Nifty Sectoral Momentum Pulse</h2>", unsafe_allow_html=True)

with col_next:
    if st.session_state.current_page < 3:
        st.button("Next Screen ➡️", on_click=next_page)

st.divider() 

# =======================================================
# PAGE 1: MARKET WATCHLIST (The established 750 list)
# =======================================================
if st.session_state.current_page == 1:
    
    df_watchlist = load_data_watchlist()
    
    st.sidebar.header("Filter & Rank Engine")
    
    sort_options = ["Market Cap (Cr)", "1M Return (%)", "1W Return (%)", "3M Return (%)", "6M Return (%)", "1Y Return (%)", "Price", "Sector"]
    sort_by = st.sidebar.selectbox("Rank Stocks By:", sort_options)
    sort_order = st.sidebar.radio("Order:", ["Descending (Top Performers/Largest)", "Ascending (Bottom Performers/Smallest)"])
    
    all_sectors = ["All"] + sorted(list(df_watchlist["Sector"].unique()))
    selected_sector = st.sidebar.selectbox("Filter by Sector:", all_sectors)
    
    min_mcap = st.sidebar.number_input("Minimum Market Cap (in Crores)", min_value=0, value=0, step=500)
    filter_dma_50 = st.sidebar.checkbox("Only show stocks Above 50 DMA")
    filter_dma_200 = st.sidebar.checkbox("Only show stocks Above 200 DMA")

    ascending = True if sort_order.startswith("Ascending") else False
    
    df_filtered = df_watchlist.copy()
    
    if selected_sector != "All":
        df_filtered = df_filtered[df_filtered["Sector"] == selected_sector]
        
    base_total = len(df_filtered)
        
    df_filtered = df_filtered[df_filtered["Market Cap (Cr)"] >= min_mcap]
    if filter_dma_50: df_filtered = df_filtered[df_filtered["Above 50 DMA?"] == "Yes"]
    if filter_dma_200: df_filtered = df_filtered[df_filtered["Above 200 DMA?"] == "Yes"]
        
    df_sorted = df_filtered.sort_values(by=sort_by, ascending=ascending).reset_index(drop=True)
    df_sorted.index = df_sorted.index + 1

    filtered_total = len(df_sorted)
    percentage = (filtered_total / base_total * 100) if base_total > 0 else 0

    st.markdown(
        f"<div style='text-align: right; font-size: 16px; font-weight: bold; padding-bottom: 10px;'>"
        f"Matches: <span style='color: #4CAF50;'>{filtered_total}</span> / {base_total} "
        f"(<span style='color: #4CAF50;'>{percentage:.1f}%</span>) of base</div>", 
        unsafe_allow_html=True
    )

    st.dataframe(df_sorted.style.format(
        formatter={
            "Market Cap (Cr)": "{:,.2f}",
            "1W Return (%)": "{:.2f}%",
            "1M Return (%)": "{:.2f}%",
            "3M Return (%)": "{:.2f}%",
            "6M Return (%)": "{:.2f}%",
            "1Y Return (%)": "{:.2f}%",
        },
        na_rep="-"
    ), use_container_width=True, height=600)

# =======================================================
# PAGE 2: NIFTY BROAD INDICES BENCHMARKING
# ==========================================
elif st.session_state.current_page == 2:
    
    broad_index_config = {
        "^NSEI": "Nifty 50",
        "^NSMIDCP": "Nifty Next 50",
        "NIFTYMIDCAP150.NS": "Nifty Midcap 150",
        "HDFCSML250.NS": "Nifty Smallcap 250",
        #"NIFTY_MICROCAP250.NS": "Nifty Microcap 250",
        "^CRSLDX": "Nifty 500",
    }
    
    df_indices, df_history = load_index_data(broad_index_config)
    
    if not df_indices.empty:
        
        timeframes = ["1W Return (%)", "1M Return (%)", "3M Return (%)", "6M Return (%)", "1Y Return (%)"]
        selected_timeframe = st.selectbox("Select Timeframe:", timeframes, index=1)
        
        df_sorted_indices = df_indices.sort_values(by=selected_timeframe, ascending=False).reset_index(drop=True)
        df_sorted_indices['Color'] = np.where(df_sorted_indices[selected_timeframe] >= 0, 'Positive', 'Negative')
        df_sorted_indices['Label'] = df_sorted_indices[selected_timeframe].apply(lambda x: f"{x:.2f}%" if not pd.isna(x) else "")
        
        lookback_map = {
            "1W Return (%)": 6,
            "1M Return (%)": 22,
            "3M Return (%)": 64,
            "6M Return (%)": 126,
            "1Y Return (%)": len(df_history)
        }
        days = lookback_map.get(selected_timeframe, 22)
        df_slice = df_history.tail(days)

        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"**Index Returns for {selected_timeframe}**")
            fig_bar = px.bar(
                df_sorted_indices,
                x="Market Index",
                y=selected_timeframe,
                text="Label",
                color="Color",
                color_discrete_map={"Positive": "#00C853", "Negative": "#FF1744"}
            )
            fig_bar.update_traces(textposition='outside')
            fig_bar.update_layout(showlegend=False, xaxis_title="", yaxis_title="Return (%)", height=500)
            st.plotly_chart(fig_bar, use_container_width=True)
            
        with col2:
            st.markdown(f"**Comparative Performance Waveform ({selected_timeframe})**")
            
            selected_lines = st.multiselect(
                "Select indices to compare:",
                options=df_history.columns,
                default=list(df_history.columns)
            )
            
            if selected_lines:
                df_slice_filtered = df_slice[selected_lines]
                df_normalized = (df_slice_filtered / df_slice_filtered.iloc[0] - 1) * 100
                df_melted = df_normalized.reset_index().melt(id_vars='Date', var_name='Index', value_name='Return (%)')

                fig_line = px.line(df_melted, x='Date', y='Return (%)', color='Index')
                fig_line.update_layout(xaxis_title="", yaxis_title="Cumulative Return (%)", height=500, legend_title_text="")
                st.plotly_chart(fig_line, use_container_width=True)
            else:
                st.info("Please select at least one index to display the waveform.")
            
    else:
        st.warning("⚠️ Could not load data for Nifty Broad Indices. The symbols might be temporarily unavailable on Yahoo Finance.")

# =======================================================
# PAGE 3: NIFTY SECTORAL MOMENTUM PULSE
# =======================================================
elif st.session_state.current_page == 3:
    
    sectoral_config = {
        "^CNXSERVICE": "Service",
        "^CNXREALTY": "Realty",
        "HDFCPVTBAN.NS": "Private Bank",
        "^CNXPHARMA": "Pharma",
        "^CNXPSUBANK": "PSU Bank",
        "OILIETF.NS": "Oil and Gas",
        "^CNXMETAL": "Metal",
        "^CNXMEDIA": "Media",
        "^CNXMNC": "MNC",
        "^CNXINFRA": "Infra",
        "^CNXCONSUM": "Consumption",
        "^CNXIT": "IT",
        "NIFTY_FIN_SERVICE.NS": "Fin Serv",
        "^CNXFMCG": "FMCG",
        "^CNXENERGY": "Energy",
        "^CNXCMDT": "Commodities",
        "^CNXPSE": "CPSE",
        "^NSEBANK": "Bank",
        "^CNXAUTO": "Auto",
        "MODEFENCE.NS": "Defence",
        "MOTOUR.NS": "Tourism",
        "MOCAPITAL.NS": "Capital Markets"   
    }
    
    df_sectors, _ = load_index_data(sectoral_config)
    
    if not df_sectors.empty:
        
        timeframes_sector = ["1W Return (%)", "1M Return (%)", "3M Return (%)", "6M Return (%)", "1Y Return (%)"]
        selected_timeframe_sector = st.selectbox("Select Momentum Timeframe:", timeframes_sector, index=1)
        
        df_sorted_sectors = df_sectors.sort_values(by=selected_timeframe_sector, ascending=False).reset_index(drop=True)
        
        stacked_display = df_sorted_sectors.copy()
        stacked_display['Color'] = np.where(stacked_display[selected_timeframe_sector] >= 0, 'Positive', 'Negative')
        stacked_display['Label'] = stacked_display[selected_timeframe_sector].apply(lambda x: f"{x:.2f}%" if not pd.isna(x) else "")
        
        fig_sectors = px.bar(
            stacked_display,
            x="Market Index",
            y=selected_timeframe_sector,
            text="Label",
            color="Color",
            color_discrete_map={"Positive": "#00C853", "Negative": "#FF1744"}
        )
        
        fig_sectors.update_traces(textposition='outside')
        fig_sectors.update_layout(showlegend=False, xaxis_title="", yaxis_title=selected_timeframe_sector, height=600)
        
        st.plotly_chart(fig_sectors, use_container_width=True)
        
        # -------------------------------------------------------------
        # NEW FEATURE: DRILL-DOWN INTO SPECIFIC SECTOR COMPONENTS
        # -------------------------------------------------------------
        st.divider()
        st.subheader("🔍 Deep Dive: Sector Constituents")
        
        # Select box to choose the sector to investigate
        drill_sector = st.selectbox("Select a Sector to view its components:", list(sectoral_config.values()))
        
        # 1. Fetch the exact constituent symbols from the official NSE CSV link
        constituent_symbols = fetch_sector_constituents(drill_sector)
        
        # 2. Get our master list of stocks (already cached from Page 1 so this is instant!)
        df_master = load_data_watchlist()
        
        if constituent_symbols:
            # Filter the master list to ONLY show stocks that belong to the NSE CSV
            df_drilled = df_master[df_master['Stock'].isin(constituent_symbols)].copy()
            
            if not df_drilled.empty:
                # Format exactly like Page 1
                df_drilled_sorted = df_drilled.sort_values(by="Market Cap (Cr)", ascending=False).reset_index(drop=True)
                df_drilled_sorted.index = df_drilled_sorted.index + 1
                
                st.markdown(f"Showing **{len(df_drilled_sorted)}** stocks from the **Nifty {drill_sector}** index.")
                
                st.dataframe(df_drilled_sorted.style.format(
                    formatter={
                        "Market Cap (Cr)": "{:,.2f}",
                        "1W Return (%)": "{:.2f}%",
                        "1M Return (%)": "{:.2f}%",
                        "3M Return (%)": "{:.2f}%",
                        "6M Return (%)": "{:.2f}%",
                        "1Y Return (%)": "{:.2f}%",
                    },
                    na_rep="-"
                ), use_container_width=True, height=400)
            else:
                st.info(f"The stocks in the {drill_sector} index are not currently in your Watchlist URL. (Note: You are currently testing with the Midcap50 URL. Switch to the Total Market URL to see all sector constituents).")
        else:
            st.warning(f"Could not fetch the official constituent list for {drill_sector}. It may not have a standard Nifty CSV link available.")
            
    else:
        st.warning("⚠️ Could not load data for Nifty Sectoral Indices.")
