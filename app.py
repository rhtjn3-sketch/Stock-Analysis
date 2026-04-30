import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import requests
import io
import time
import plotly.express as px  

st.set_page_config(page_title="Nifty 750 Pro Engine", layout="wide")

# ==========================================
# PAGE ROUTING SYSTEM (Next/Previous Logic)
# ==========================================
if 'current_page' not in st.session_state:
    st.session_state.current_page = 1

def next_page():
    if st.session_state.current_page < 4: 
        st.session_state.current_page += 1

def prev_page():
    if st.session_state.current_page > 1:
        st.session_state.current_page -= 1

# =======================================================
# CORE DATA ENGINE: Multi-Pass Healing Downloader
# =======================================================
@st.cache_data(show_spinner=False)
def fetch_market_data_bulk():
    """
    Pass 1 executes a fast bulk download. Pass 2 audits the results 
    and rescues any stocks dropped by Yahoo's rate limits.
    """
    raw_tickers = []
    industry_map = {}
    
    total_market_url = "https://www.niftyindices.com/IndexConstituent/ind_niftytotalmarket_list.csv"
    #total_market_url = "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap50list.csv"
    
    headers = {"User-Agent": "Mozilla/5.0"}

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
        pass
    
    if not raw_tickers:
        raw_tickers = ["RELIANCE", "TCS", "HDFCBANK", "ZOMATO", "SUZLON", "PAYTM"]
        industry_map = {t: "Unknown" for t in raw_tickers}                                                  
        
    tickers = [ticker if ticker.endswith('.NS') else f"{ticker}.NS" for ticker in raw_tickers]
	
														 
																		  
	
							   
    total_tickers = len(tickers)
    
    # --- PASS 1: The Bulk Haul ---
    with st.spinner(f"Pass 1: Fast bulk download of {total_tickers} stocks..."):
        data_main = yf.download(tickers, period="2y", group_by='ticker', threads=7, progress=False)
        
    # --- AUDIT: Identify Dropped Stocks ---
    if isinstance(data_main.columns, pd.MultiIndex):
        downloaded_tickers = data_main.columns.get_level_values(0).unique().tolist()
    else:
        downloaded_tickers = [] if data_main.empty else [tickers[0]]
        
    missing_tickers = [t for t in tickers if t not in downloaded_tickers]
    
    # --- PASS 2: The Rescue Mission ---
    if missing_tickers:
        with st.spinner(f"Pass 2: Rescuing {len(missing_tickers)} dropped stocks..."):
            rescue_frames = []
            chunk_size = 20 # Small chunks to stay under the radar
										   
																					   
            
            for i in range(0, len(missing_tickers), chunk_size):
                chunk = missing_tickers[i:i+chunk_size]
                # threads=False here to prevent further rate limits on the rescue chunks
                chunk_data = yf.download(chunk, period="2y", group_by='ticker', threads=False, progress=False)
                
                if not chunk_data.empty:
                    # Fix yfinance edge case where 1 surviving stock drops the MultiIndex
                    if not isinstance(chunk_data.columns, pd.MultiIndex):
                        if len(chunk) == 1:
                            chunk_data.columns = pd.MultiIndex.from_product([chunk, chunk_data.columns])
                        else:
                            # Fallback if multiple were requested but only 1 survived
                            chunk_data.columns = pd.MultiIndex.from_product([[chunk[0]], chunk_data.columns])
                    rescue_frames.append(chunk_data)
                time.sleep(0.5) 
                
            if rescue_frames:
                data_rescue = pd.concat(rescue_frames, axis=1)
                final_data = pd.concat([data_main, data_rescue], axis=1)
            else:
                final_data = data_main
    else:
        final_data = data_main
    
    return tickers, final_data, industry_map

# =======================================================
# DATA ENGINE 1: Fetching & Filtering (for Watchlist)
# =======================================================
@st.cache_data 
def load_data_watchlist():
																			
    tickers, data, industry_map = fetch_market_data_bulk()
        
    total_tickers = len(tickers)
    progress_text = "Fetching Fundamentals & Market Cap. Please wait..."
    my_bar = st.progress(0, text=progress_text)
    
    metrics = []
    failed_tickers = []
    
    for i, ticker in enumerate(tickers):
        my_bar.progress((i + 1) / total_tickers, text=f"Processing {i+1}/{total_tickers}: {ticker}")
        try:
            df = data[ticker].dropna() if len(tickers) > 1 else data.dropna()
            if df.empty: continue
            
            clean_symbol = ticker.replace('.NS', '')                                       
            
														 
            tkr = yf.Ticker(ticker)
            stock_info = tkr.info
            market_cap_raw = stock_info.get('marketCap')
            
																					   
            if not market_cap_raw or market_cap_raw == 0:
                try:
                    market_cap_raw = tkr.fast_info.market_cap
                except Exception:
                    market_cap_raw = 0

            sector = industry_map.get(clean_symbol, 'Unknown')
            
            if market_cap_raw is None or market_cap_raw == 0: 
                market_cap_cr = 0.0
            else: 
                market_cap_cr = round(market_cap_raw / 10000000, 2)
            
            current_price = df['Close'].iloc[-1]
            
            ret_1w = (current_price / df['Close'].iloc[-6] - 1) * 100 if len(df) >= 6 else np.nan
            ret_1m = (current_price / df['Close'].iloc[-22] - 1) * 100 if len(df) >= 22 else np.nan
            ret_3m = (current_price / df['Close'].iloc[-64] - 1) * 100 if len(df) >= 64 else np.nan
            ret_6m = (current_price / df['Close'].iloc[-126] - 1) * 100 if len(df) >= 126 else np.nan
            ret_1y = (current_price / df['Close'].iloc[-250] - 1) * 100 if len(df) >= 250 else np.nan
            
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
    
    # --- ANTI-CRASH FIX: Returns empty dataframe with correct columns if completely blocked ---
    if not metrics:
        return pd.DataFrame(columns=[
            "Stock", "Sector", "Market Cap (Cr)", "Price", 
            "1W Return (%)", "1M Return (%)", "3M Return (%)", 
            "6M Return (%)", "1Y Return (%)", "Above 50 DMA?", "Above 200 DMA?"
        ])
        
    return pd.DataFrame(metrics)

# =======================================================
# DATA ENGINE 2: Fetching Index Performance
# =======================================================
@st.cache_data
def load_index_data(index_list_with_names):
    symbol_to_name = {ticker: name for ticker, name in index_list_with_names.items()}
    tickers = list(symbol_to_name.keys())
    
    with st.spinner(f'Downloading performance data for market indices...'):
        data = yf.download(tickers, period="1y", group_by='ticker', threads=True)
    
    results = []
    history_dict = {} 
    
    for i, ticker in enumerate(tickers):
        try:
            df = data[ticker].dropna() if len(tickers) > 1 else data
            if df.empty: continue
            
            index_name = symbol_to_name.get(ticker, ticker)
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
    df_history = pd.DataFrame(history_dict)
    
    return df_metrics, df_history

# =======================================================
# DATA ENGINE 3: Fetching Specific Sector Constituents
# =======================================================
@st.cache_data
def fetch_sector_constituents(sector_name):
    url_map = {
        "Auto": "ind_niftyautolist.csv", "Bank": "ind_niftybanklist.csv", "IT": "ind_niftyitlist.csv",
        "Pharma": "ind_niftypharmalist.csv", "Realty": "ind_niftyrealtylist.csv", "FMCG": "ind_niftyfmcglist.csv",
        "Metal": "ind_niftymetallist.csv", "Energy": "ind_niftyenergylist.csv", "Infra": "ind_niftyinfralist.csv",
        "MNC": "ind_niftymnclist.csv", "PSU Bank": "ind_niftypsubanklist.csv", "Private Bank": "ind_nifty_privatebanklist.csv",
        "Media": "ind_niftymedialist.csv", "Fin Serv": "ind_niftyfinancelist.csv", "Commodities": "ind_niftycommoditieslist.csv",
        "Consumption": "ind_niftyconsumptionlist.csv", "CPSE": "ind_niftycpselist.csv", "Service": "ind_niftyservicelist.csv",
        "Oil and Gas": "ind_niftyoilgaslist.csv", "Defence" : "ind_niftyindiadefence_list.csv", "Capital Markets": "ind_niftyCapitalMarkets_list.csv",
        "Tourism": "ind_niftyindiatourism_list.csv", "Healthcare": "ind_niftyhealthcarelist.csv"
    }
    file_name = url_map.get(sector_name)
    if not file_name: return []
        
    url = f"https://www.niftyindices.com/IndexConstituent/{file_name}"
    try:
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        df = pd.read_csv(io.StringIO(res.text))
        df.columns = df.columns.str.strip()
        return df['Symbol'].astype(str).str.strip().tolist()
    except Exception: return []

# =======================================================
# DATA ENGINE 4: Historical Price Volume Scanner
# =======================================================
@st.cache_data
def get_price_volume_history():
	   
																	   
																  
	   
    tickers, data, industry_map = fetch_market_data_bulk()
    history_records = {}
    
    with st.spinner("Calculating 30-Day Historical Price & Volume signals..."):
        for ticker in tickers:
            try:
                df = data[ticker].dropna() if len(tickers) > 1 else data.dropna()
                if df.empty or len(df) < 25: continue
                
                close = df['Close']
                vol = df['Volume']
                
																   
                pct_change = (close / close.shift(1) - 1) * 100
                avg_vol = vol.rolling(window=20).mean()
                vol_surge = vol / avg_vol
                
																		   
                df_history = pd.DataFrame({
                    'Close': close,
                    'Volume': vol,
                    'Avg_Volume': avg_vol,
                    'Pct_Change': pct_change,
                    'Vol_Surge': vol_surge
                }).tail(30)
                
                clean_symbol = ticker.replace('.NS', '')
                history_records[clean_symbol] = {
                    'Sector': industry_map.get(clean_symbol, 'Unknown'),
                    'History': df_history
                }
            except Exception:
                pass
                
    return history_records

# ==========================================
# UI: TOP NAVIGATION BAR
# ==========================================
st.markdown("<br>", unsafe_allow_html=True) 
col_prev, col_title, col_next = st.columns([1, 8, 1])

with col_prev:
    if st.session_state.current_page > 1: st.button("⬅️ Previous Screen", on_click=prev_page)

with col_title:
    if st.session_state.current_page == 1: st.markdown("<h2 style='text-align: center;'>Part 1: Market Watchlist</h2>", unsafe_allow_html=True)
    elif st.session_state.current_page == 2: st.markdown("<h2 style='text-align: center;'>Part 2: Nifty Broad Indices Benchmarking</h2>", unsafe_allow_html=True)
    elif st.session_state.current_page == 3: st.markdown("<h2 style='text-align: center;'>Part 3: Nifty Sectoral Momentum Pulse</h2>", unsafe_allow_html=True)
    elif st.session_state.current_page == 4: st.markdown("<h2 style='text-align: center;'>Part 4: Price-Volume Action Screener</h2>", unsafe_allow_html=True)

with col_next:
    if st.session_state.current_page < 4: st.button("Next Screen ➡️", on_click=next_page)

st.divider() 

# =======================================================
# PAGE 1: MARKET WATCHLIST
# =======================================================
if st.session_state.current_page == 1:
    df_watchlist = load_data_watchlist()
											 
	
																																			  
																   
																											 
	
																		 
																			
    
    # --- ANTI-CRASH FIX: Stops app from crashing if data is blocked ---
    if df_watchlist.empty:
        st.warning("⚠️ Market data could not be loaded. Yahoo Finance may be temporarily blocking connections. Please wait a few minutes and refresh.")
    else:
        st.sidebar.header("Filter & Rank Engine")
	
									 
																									
        
        sort_options = ["Market Cap (Cr)", "1M Return (%)", "1W Return (%)", "3M Return (%)", "6M Return (%)", "1Y Return (%)", "Price", "Sector"]
        sort_by = st.sidebar.selectbox("Rank Stocks By:", sort_options)
        sort_order = st.sidebar.radio("Order:", ["Descending (Top Performers)", "Ascending (Bottom Performers)"])
        
        all_sectors = ["All"] + sorted(list(df_watchlist["Sector"].unique()))
        selected_sector = st.sidebar.selectbox("Filter by Sector:", all_sectors)
																						
        
        min_mcap = st.sidebar.number_input("Minimum Market Cap (in Crores)", min_value=0, value=0, step=500)
        filter_dma_50 = st.sidebar.checkbox("Only show stocks Above 50 DMA")
        filter_dma_200 = st.sidebar.checkbox("Only show stocks Above 200 DMA")
    
        ascending = True if sort_order.startswith("Ascending") else False
        
        df_filtered = df_watchlist.copy()
        if selected_sector != "All": df_filtered = df_filtered[df_filtered["Sector"] == selected_sector]
            
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
                "Price": "{:,.2f}", 
                "1W Return (%)": "{:.2f}%", "1M Return (%)": "{:.2f}%",
                "3M Return (%)": "{:.2f}%", "6M Return (%)": "{:.2f}%", "1Y Return (%)": "{:.2f}%",
            }, na_rep="-"
        ), use_container_width=True, height=600)

# =======================================================
# PAGE 2: NIFTY BROAD INDICES
# ==========================================
elif st.session_state.current_page == 2:
    broad_index_config = {
        "^NSEI": "Nifty 50", "^NSMIDCP": "Nifty Next 50", "NIFTYMIDCAP150.NS": "Nifty Midcap 150",
        "HDFCSML250.NS": "Nifty Smallcap 250", "^CRSLDX": "Nifty 500",
    }
    
    df_indices, df_history = load_index_data(broad_index_config)
    
    if not df_indices.empty:
        timeframes = ["1W Return (%)", "1M Return (%)", "3M Return (%)", "6M Return (%)", "1Y Return (%)"]
        selected_timeframe = st.selectbox("Select Timeframe:", timeframes, index=1)
        
        df_sorted_indices = df_indices.sort_values(by=selected_timeframe, ascending=False).reset_index(drop=True)
        df_sorted_indices['Color'] = np.where(df_sorted_indices[selected_timeframe] >= 0, 'Positive', 'Negative')
        df_sorted_indices['Label'] = df_sorted_indices[selected_timeframe].apply(lambda x: f"{x:.2f}%" if not pd.isna(x) else "")
        
        lookback_map = {"1W Return (%)": 6, "1M Return (%)": 22, "3M Return (%)": 64, "6M Return (%)": 126, "1Y Return (%)": len(df_history)}
        df_slice = df_history.tail(lookback_map.get(selected_timeframe, 22))

        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"**Index Returns for {selected_timeframe}**")
            fig_bar = px.bar(df_sorted_indices, x="Market Index", y=selected_timeframe, text="Label", color="Color", color_discrete_map={"Positive": "#00C853", "Negative": "#FF1744"})
            fig_bar.update_traces(textposition='outside')
            fig_bar.update_layout(showlegend=False, xaxis_title="", yaxis_title="Return (%)", height=500)
            st.plotly_chart(fig_bar, use_container_width=True)
            
        with col2:
            st.markdown(f"**Comparative Performance Waveform ({selected_timeframe})**")
            selected_lines = st.multiselect("Select indices to compare:", options=df_history.columns, default=list(df_history.columns))
            
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
        st.warning("⚠️ Could not load data for Nifty Broad Indices.")

# =======================================================
# PAGE 3: NIFTY SECTORAL MOMENTUM & DEEP DIVE
# =======================================================
elif st.session_state.current_page == 3:
    
    sectoral_config = {
        "^CNXSERVICE": "Service", "^CNXREALTY": "Realty", "HDFCPVTBAN.NS": "Private Bank",
        "^CNXPHARMA": "Pharma", "^CNXPSUBANK": "PSU Bank", "OILIETF.NS": "Oil and Gas",
        "^CNXMETAL": "Metal", "^CNXMEDIA": "Media", "^CNXMNC": "MNC",
        "^CNXINFRA": "Infra", "^CNXCONSUM": "Consumption", "^CNXIT": "IT",
        "NIFTY_FIN_SERVICE.NS": "Fin Serv", "^CNXFMCG": "FMCG", "^CNXENERGY": "Energy",
        "^CNXCMDT": "Commodities", "CPSEETF.NS": "CPSE", "^NSEBANK": "Bank",
        "^CNXAUTO": "Auto", "MODEFENCE.NS": "Defence", "MOTOUR.NS": "Tourism",
		"MOCAPITAL.NS": "Capital Markets", "AXISHCETF.NS": "Healthcare" 
    }
    
    df_sectors, _ = load_index_data(sectoral_config)
    
    if not df_sectors.empty:
        timeframes_sector = ["1W Return (%)", "1M Return (%)", "3M Return (%)", "6M Return (%)", "1Y Return (%)"]
        selected_timeframe_sector = st.selectbox("Select Momentum Timeframe:", timeframes_sector, index=1)
        
        df_sorted_sectors = df_sectors.sort_values(by=selected_timeframe_sector, ascending=False).reset_index(drop=True)
        stacked_display = df_sorted_sectors.copy()
        stacked_display['Color'] = np.where(stacked_display[selected_timeframe_sector] >= 0, 'Positive', 'Negative')
        stacked_display['Label'] = stacked_display[selected_timeframe_sector].apply(lambda x: f"{x:.2f}%" if not pd.isna(x) else "")
        
        fig_sectors = px.bar(stacked_display, x="Market Index", y=selected_timeframe_sector, text="Label", color="Color", color_discrete_map={"Positive": "#00C853", "Negative": "#FF1744"})
        fig_sectors.update_traces(textposition='outside')
        fig_sectors.update_layout(showlegend=False, xaxis_title="", yaxis_title=selected_timeframe_sector, height=600)
        st.plotly_chart(fig_sectors, use_container_width=True)
        
																	   
																 
																	   
        st.divider()
        st.subheader("🔍 Deep Dive: Sector Constituents")
        
        drill_sector = st.selectbox("Select a Sector to view its components:", list(sectoral_config.values()))
        constituent_symbols = fetch_sector_constituents(drill_sector)
        df_master = load_data_watchlist()
        
        if constituent_symbols:
            df_drilled = df_master[df_master['Stock'].isin(constituent_symbols)].copy()
            
            if not df_drilled.empty:
                return_cols = ["1W Return (%)", "1M Return (%)", "3M Return (%)", "6M Return (%)", "1Y Return (%)"]
                sector_stats = {}
                for col in return_cols:
                    sector_stats[col] = {'mean': df_drilled[col].mean(), 'std': df_drilled[col].std()}
                
                col_empty, col_filters = st.columns([4, 1]) 
                with col_filters:
                    st.markdown("**Technical Filters:**")
                    dd_filter_50 = st.checkbox("Above 50 DMA ? (Yes/No)", key="dd_50")
                    dd_filter_200 = st.checkbox("Above 200 DMA ? (Yes/No)", key="dd_200")
                
                if dd_filter_50: df_drilled = df_drilled[df_drilled["Above 50 DMA?"] == "Yes"]
                if dd_filter_200: df_drilled = df_drilled[df_drilled["Above 200 DMA?"] == "Yes"]
                
                df_drilled_sorted = df_drilled.sort_values(by="Market Cap (Cr)", ascending=False).reset_index(drop=True)
                df_drilled_sorted.index = df_drilled_sorted.index + 1
                
                st.markdown(f"Showing **{len(df_drilled_sorted)}** stocks from the **Nifty {drill_sector}** index.")
                
                def apply_z_colors(s):
                    if s.name in return_cols:
                        mean = sector_stats[s.name]['mean']
                        std = sector_stats[s.name]['std']
                        colors = []
                        for val in s:
                            if pd.isna(val) or std == 0 or pd.isna(std):
                                colors.append('')
                                continue
                            z = (val - mean) / std
                            if z >= 1.0: colors.append('background-color: #1b5e20; color: white;')      
                            elif z >= 0.5: colors.append('background-color: #4caf50; color: white;')    
                            elif z > 0: colors.append('background-color: #a5d6a7; color: black;')       
                            elif z >= -0.5: colors.append('background-color: #ffe0b2; color: black;')   
                            elif z >= -1.0: colors.append('background-color: #ff9800; color: black;')   
                            elif z >= -1.5: colors.append('background-color: #f57c00; color: white;')   
                            elif z >= -2.0: colors.append('background-color: #ef9a9a; color: black;')   
                            elif z >= -2.5: colors.append('background-color: #f44336; color: white;')   
                            else: colors.append('background-color: #b71c1c; color: white;')             
                        return colors
                    return ['' for _ in s]
                
                styled_df = df_drilled_sorted.style.apply(apply_z_colors, axis=0).format(
                    formatter={
                        "Market Cap (Cr)": "{:,.2f}", 
                        "Price": "{:,.2f}", 
                        "1W Return (%)": "{:.2f}%", "1M Return (%)": "{:.2f}%",
                        "3M Return (%)": "{:.2f}%", "6M Return (%)": "{:.2f}%", "1Y Return (%)": "{:.2f}%"
                    }, na_rep="-"
                )
                st.dataframe(styled_df, use_container_width=True, height=400)
            else:
                st.info(f"The stocks in the {drill_sector} index are not currently in your Watchlist URL.")
        else:
            st.warning(f"Could not fetch the official constituent list for {drill_sector}.")
    else:
        st.warning("⚠️ Could not load data for Nifty Sectoral Indices.")

# =======================================================
# PAGE 4: PRICE-VOLUME ACTION SCREENER
# =======================================================
elif st.session_state.current_page == 4:
    st.sidebar.header("Price-Volume Parameters")
    
																							  
    history_records = get_price_volume_history()
    
    if not history_records:
        st.warning("⚠️ Market data is still initializing or unavailable.")
    else:
																		   
        first_stock = list(history_records.keys())[0]
        available_dates = history_records[first_stock]['History'].index.strftime('%Y-%m-%d').tolist()
        
										 
        selected_date_str = st.sidebar.selectbox("Select Target Date:", sorted(available_dates, reverse=True))
        
							  
        vol_multiplier = st.sidebar.number_input("Minimum Volume Surge (x Usual)", min_value=1.0, value=2.0, step=0.5)
        price_surge_pct = st.sidebar.number_input("Minimum Price Surge (%)", min_value=0.5, value=3.0, step=0.5)
        
        results = []
        
												
        for stock, info in history_records.items():
            df_hist = info['History']
            
																	 
            df_hist_str = df_hist.copy()
            df_hist_str.index = df_hist_str.index.strftime('%Y-%m-%d')
            
            if selected_date_str in df_hist_str.index:
                row = df_hist_str.loc[selected_date_str]
                
																			
                if row['Pct_Change'] >= price_surge_pct and row['Vol_Surge'] >= vol_multiplier:
                    
																								  
                    action_dates_df = df_hist_str[(df_hist_str['Pct_Change'] >= price_surge_pct) & (df_hist_str['Vol_Surge'] >= vol_multiplier)]
                    action_count = len(action_dates_df)
                    
																		 
                    dates_str = ", ".join(action_dates_df.index.tolist())
                    display_val = f"{action_count} ({dates_str})"
                    
                    results.append({
                        "Stock": stock,
                        "Sector": info['Sector'],
                        "Close Price": row['Close'],
                        "Volume": row['Volume'],
                        "20D Avg Volume": row['Avg_Volume'],
                        "Volume Surge (x)": row['Vol_Surge'],
                        "Price Change (%)": row['Pct_Change'],
                        "Action Count (Last 30 Days)": display_val  
                    })
                    
							   
        if results:
            df_results = pd.DataFrame(results)
													  
            df_results = df_results.sort_values(by="Volume Surge (x)", ascending=False).reset_index(drop=True)
            df_results.index = df_results.index + 1
            
            st.markdown(f"### Stocks showing Price-Volume Action on **{selected_date_str}**")
            st.markdown(f"> **Criteria Met:** Price increased by **≥ {price_surge_pct}%** |  Volume was **≥ {vol_multiplier}x** its 20-Day Average")
            
										  
            st.dataframe(df_results.style.format({
                "Close Price": "{:,.2f}", 
                "Volume": "{:,.0f}",
                "20D Avg Volume": "{:,.0f}",
                "Volume Surge (x)": "{:.2f}x",
                "Price Change (%)": "{:.2f}%"
            }), use_container_width=True, height=600)
            
        else:
            st.info(f"No stocks in your watchlist met the criteria on {selected_date_str}.")
