import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import requests
import io
import time
import plotly.express as px  
import concurrent.futures
import os

st.set_page_config(page_title="Nifty 750 Pro Engine", layout="wide")

# ==========================================
# CONSTANTS & CONFIGURATIONS
# ==========================================
GOOGLE_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRoR_4ZAlo5CY1NB-QsDJPqfeswra43fvKRHCF6sL9AlP9dxHRUAmV9MhWVRMKs5a-UB5faWcQblc9B/pub?gid=0&single=true&output=csv"

broad_index_config = {
    "^NSEI": "Nifty 50", "^NSMIDCP": "Nifty Next 50", "NIFTYMIDCAP150.NS": "Nifty Midcap 150",
    "HDFCSML250.NS": "Nifty Smallcap 250", "^CRSLDX": "Nifty 500",
}

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

world_index_config = {
    "^KS11": "Kospi (Korea)", "^N225": "Nikkei (Japan)", "^BVSP": "Bovespa (Brazil)",
    "^DJI": "Dow Jones", "^FTSE": "FTSE (UK)", "^RUT": "Russell (US)", "NQ=F": "Nasdaq 100", 
    "^GSPC": "S&P 500", "^BSESN": "Sensex", "^FCHI": "CAC 40 (Europe)", "^GDAXI": "DAX (Germany)", 
    "^HSI": "Hang Seng (China)", "^MXX": "MXX (Mexico)", "^STOXX50E": "Stoxx 50 (Europe)", 
    "^STI": "STI (Singapore)", "^TWII": "TWSE (Taiwan)", "^AXJO": "ASX 200 (Australia)", 
    "^GSPTSE": "TSX (Canada)", "^NSEI": "Nifty"
}

# ==========================================
# GLOBAL DATA ENGINES (Available to all sheets)
# ==========================================
@st.cache_data(show_spinner=False)
def fetch_market_data_bulk():
    try:
        final_data = pd.read_parquet("nifty_750_master.parquet")
        final_data.index = pd.to_datetime(final_data.index).tz_localize(None)
        tickers = list(final_data.columns.levels[0])
    except FileNotFoundError:
        final_data = pd.DataFrame()
        tickers = []
    return tickers, final_data

@st.cache_data 
def load_data_watchlist():
    tickers, data = fetch_market_data_bulk()
    if data.empty: return pd.DataFrame()
    
    mcap_dict, sector_dict = {}, {}
    try:
        df_sheet = pd.read_csv(GOOGLE_SHEET_CSV_URL)
        for _, row in df_sheet.iterrows():
            sym = str(row.get('Stock Name', '')).strip()
            sec = str(row.get('Sector', 'Unknown')).strip()
            try: mcap = float(row.get('Market Cap (in Cr)', 0.0))
            except: mcap = 0.0
            mcap_dict[sym] = mcap; sector_dict[sym] = sec
    except Exception: pass
        
    total_tickers = len(tickers)
    my_bar = st.progress(0, text="Compiling Watchlist Metrics...")
    metrics = []
    
    for i, ticker in enumerate(tickers):
        my_bar.progress((i + 1) / total_tickers, text=f"Processing {i+1}/{total_tickers}: {ticker}")
        try:
            df = data[ticker].dropna() if len(tickers) > 1 else data.dropna()
            if df.empty: continue
            
            clean_symbol = ticker.replace('.NS', '')
            current_price = df['Close'].iloc[-1]
            
            ret_1d = (current_price / df['Close'].iloc[-2] - 1) * 100 if len(df) >= 2 else np.nan
            ret_1w = (current_price / df['Close'].iloc[-6] - 1) * 100 if len(df) >= 6 else np.nan
            ret_1m = (current_price / df['Close'].iloc[-22] - 1) * 100 if len(df) >= 22 else np.nan
            ret_3m = (current_price / df['Close'].iloc[-64] - 1) * 100 if len(df) >= 64 else np.nan
            ret_6m = (current_price / df['Close'].iloc[-126] - 1) * 100 if len(df) >= 126 else np.nan
            ret_1y = (current_price / df['Close'].iloc[-250] - 1) * 100 if len(df) >= 250 else np.nan
            
            sma_50 = df['Close'].rolling(window=50).mean().iloc[-1] if len(df) >= 50 else np.nan
            sma_200 = df['Close'].rolling(window=200).mean().iloc[-1] if len(df) >= 200 else np.nan
             
            metrics.append({
                "Stock": clean_symbol, "Sector": sector_dict.get(clean_symbol, 'Unknown'),
                "Market Cap (Cr)": mcap_dict.get(clean_symbol, 0.0), "Price": round(current_price, 2),
                "1D Return (%)": round(ret_1d, 2) if not np.isnan(ret_1d) else None,
                "1W Return (%)": round(ret_1w, 2) if not np.isnan(ret_1w) else None,
                "1M Return (%)": round(ret_1m, 2) if not np.isnan(ret_1m) else None,
                "3M Return (%)": round(ret_3m, 2) if not np.isnan(ret_3m) else None,
                "6M Return (%)": round(ret_6m, 2) if not np.isnan(ret_6m) else None,
                "1Y Return (%)": round(ret_1y, 2) if not np.isnan(ret_1y) else None,
                "Above 50 DMA?": "Yes" if not np.isnan(sma_50) and current_price > sma_50 else "No",
                "Above 200 DMA?": "Yes" if not np.isnan(sma_200) and current_price > sma_200 else "No"
            })
        except Exception: pass
    my_bar.empty()
    return pd.DataFrame(metrics)

@st.cache_data(ttl=300)
def sync_and_get_metrics(parquet_filename, config_dict):
    """Universal Delta Sync Engine"""
    symbol_to_name = {ticker: name for ticker, name in config_dict.items()}
    tickers = list(symbol_to_name.keys())
    
    try:
        hist_df = pd.read_parquet(parquet_filename)
        hist_df.index = pd.to_datetime(hist_df.index).tz_localize(None)
        available_cols = [t for t in tickers if t in hist_df.columns]
        hist_df = hist_df[available_cols]
    except Exception:
        hist_df = pd.DataFrame()
        
    live_frames = []
    for ticker in tickers:
        for attempt in range(2): 
            try:
                df = yf.Ticker(ticker).history(period="1d")
                if not df.empty:
                    df = df[['Close']].copy()
                    df.columns = [ticker]
                    df.index = pd.to_datetime(df.index).tz_localize(None)
                    live_frames.append(df)
                    break
            except Exception: pass
            time.sleep(0.2)
            
    if live_frames:
        live_df = pd.concat(live_frames, axis=1)
        if not hist_df.empty:
            merged_df = pd.concat([hist_df, live_df])
            merged_df = merged_df[~merged_df.index.duplicated(keep='last')] 
        else: merged_df = live_df
    else: merged_df = hist_df
        
    if merged_df.empty: return pd.DataFrame(), pd.DataFrame()
    merged_df = merged_df.ffill().dropna(how='all')
    
    results = []
    history_dict = {} 
    
    for ticker in merged_df.columns:
        if ticker not in symbol_to_name: continue
        index_name = symbol_to_name.get(ticker)
        clean_series = merged_df[ticker].dropna()
        if clean_series.empty: continue
            
        history_dict[index_name] = clean_series
        current_price = clean_series.iloc[-1]
        
        ret_1d = (current_price / clean_series.iloc[-2] - 1) * 100 if len(clean_series) >= 2 else np.nan
        ret_1w = (current_price / clean_series.iloc[-6] - 1) * 100 if len(clean_series) >= 6 else np.nan
        ret_1m = (current_price / clean_series.iloc[-22] - 1) * 100 if len(clean_series) >= 22 else np.nan
        ret_3m = (current_price / clean_series.iloc[-64] - 1) * 100 if len(clean_series) >= 64 else np.nan
        ret_6m = (current_price / clean_series.iloc[-126] - 1) * 100 if len(clean_series) >= 126 else np.nan
        ret_1y = (current_price / clean_series.iloc[0] - 1) * 100 if len(clean_series) > 0 else np.nan
        
        results.append({
            "Index Ticker": ticker, "Market Index": index_name, "Price": round(current_price, 2),
            "1D Return (%)": round(ret_1d, 2), "1W Return (%)": round(ret_1w, 2), "1M Return (%)": round(ret_1m, 2),
            "3M Return (%)": round(ret_3m, 2), "6M Return (%)": round(ret_6m, 2), "1Y Return (%)": round(ret_1y, 2),
        })
        
    return pd.DataFrame(results), pd.DataFrame(history_dict)

@st.cache_data
def fetch_sector_constituents(name):
    url_map = {"Auto": "ind_niftyautolist.csv", "Bank": "ind_niftybanklist.csv", "IT": "ind_niftyitlist.csv", "Pharma": "ind_niftypharmalist.csv", "Realty": "ind_niftyrealtylist.csv", "FMCG": "ind_niftyfmcglist.csv", "Metal": "ind_niftymetallist.csv", "Energy": "ind_niftyenergylist.csv", "Infra": "ind_niftyinfralist.csv", "MNC": "ind_niftymnclist.csv", "PSU Bank": "ind_niftypsubanklist.csv", "Private Bank": "ind_nifty_privatebanklist.csv", "Media": "ind_niftymedialist.csv", "Fin Serv": "ind_niftyfinancelist.csv", "Commodities": "ind_niftycommoditieslist.csv", "Consumption": "ind_niftyconsumptionlist.csv", "CPSE": "ind_niftycpselist.csv", "Service": "ind_niftyservicelist.csv", "Oil and Gas": "ind_niftyoilgaslist.csv", "Defence" : "ind_niftyindiadefence_list.csv", "Capital Markets": "ind_niftyCapitalMarkets_list.csv", "Tourism": "ind_niftyindiatourism_list.csv", "Healthcare": "ind_niftyhealthcarelist.csv"}
    f = url_map.get(name)
    if not f: return []
    try:
        res = requests.get(f"https://www.niftyindices.com/IndexConstituent/{f}", headers={"User-Agent": "Mozilla/5.0"})
        df = pd.read_csv(io.StringIO(res.text))
        return df['Symbol'].astype(str).str.strip().tolist()
    except: return []

# =======================================================
# MOMENTUM MATRIX STYLER
# =======================================================
def get_rank_color(val, max_rank):
    """Discrete Hex mapping matching Sheet 3 Deep Dive colors."""
    try:
        if pd.isna(val) or val == "-": return ""
        rank = int(str(val).split(' ')[0])
        if max_rank <= 1: return "background-color: #a5d6a7; color: black;"
        
        # Calculate where this rank falls on a scale of 0.0 (Top) to 1.0 (Bottom)
        ratio = (rank - 1) / (max_rank - 1)
        
        # Map to the 9 soothing Hex codes from Sheet 3
        if ratio <= 0.11:   return 'background-color: #1b5e20; color: white;'  # Dark Green
        elif ratio <= 0.22: return 'background-color: #4caf50; color: white;'  # Green
        elif ratio <= 0.33: return 'background-color: #a5d6a7; color: black;'  # Light Green
        elif ratio <= 0.44: return 'background-color: #ffe0b2; color: black;'  # Light Orange/Neutral
        elif ratio <= 0.55: return 'background-color: #ff9800; color: black;'  # Orange
        elif ratio <= 0.66: return 'background-color: #f57c00; color: white;'  # Dark Orange
        elif ratio <= 0.77: return 'background-color: #ef9a9a; color: black;'  # Light Red
        elif ratio <= 0.88: return 'background-color: #f44336; color: white;'  # Red
        else:               return 'background-color: #b71c1c; color: white;'  # Dark Red
    except: 
        return ""

def render_momentum_matrix(df_metrics, title):
    cols_to_rank = ['1W Return (%)', '1M Return (%)', '3M Return (%)', '6M Return (%)', '1Y Return (%)']
    df_ranks = df_metrics[['Market Index']].copy()
    
    for col in cols_to_rank:
        if col in df_metrics.columns:
            # Safe ranking to handle empty data gracefully
            if df_metrics[col].notna().any():
                df_ranks[col.replace('Return (%)', 'Rank')] = df_metrics[col].rank(ascending=False, method='min')
            else:
                df_ranks[col.replace('Return (%)', 'Rank')] = np.nan
        
    df_display = df_metrics[['Market Index']].copy()
    
    def format_cell(rank_val, ret_val):
        if pd.isna(rank_val) or pd.isna(ret_val): return "-"
        r = str(rank_val).split('.')[0]
        pct = f"+{ret_val:.2f}%" if ret_val > 0 else f"{ret_val:.2f}%"
        return f"{r} ({pct})"

    for col in cols_to_rank:
        if col not in df_metrics.columns: continue
        rank_col = col.replace('Return (%)', 'Rank')
        disp_col = col.replace('Return (%)', 'Momentum')
        df_display[disp_col] = [format_cell(r, v) for r, v in zip(df_ranks[rank_col], df_metrics[col])]
        df_display[col + '_raw'] = df_ranks[rank_col]
        
    if '1W Return (%)_raw' in df_display.columns:
        df_display = df_display.sort_values(by='1W Return (%)_raw', na_position='last').reset_index(drop=True)
        
    max_rank = len(df_display)
    cols_to_drop = [c for c in df_display.columns if '_raw' in c]
    df_clean = df_display.drop(columns=cols_to_drop)
    
    st.markdown(f"### 🏆 {title}")
    subset_cols = [c for c in df_clean.columns if 'Momentum' in c]
    if hasattr(df_clean.style, "map"):
        styled = df_clean.style.map(lambda v: get_rank_color(v, max_rank), subset=subset_cols)
    else:
        styled = df_clean.style.applymap(lambda v: get_rank_color(v, max_rank), subset=subset_cols)
        
    st.dataframe(styled, use_container_width=True, height=(max_rank * 36) + 40)

# ==========================================
# PAGE ROUTING & NAVIGATION
# ==========================================
if 'current_page' not in st.session_state: st.session_state.current_page = 1
def change_page(num): st.session_state.current_page = num

st.markdown("<br>", unsafe_allow_html=True) 
col_prev, col_title, col_next = st.columns([1, 8, 1])

with col_prev:
    if st.session_state.current_page > 1: st.button("⬅️ Previous", on_click=change_page, args=(st.session_state.current_page - 1,))

with col_title:
    titles = {
        1: "Part 1: Market Watchlist", 2: "Part 2: Broad Indices Benchmarking", 3: "Part 3: Sectoral Deep Dive",
        4: "Part 4: Sectoral Momentum Matrix", 5: "Part 5: Price-Volume Screener", 6: "Part 6: Macro Indicators",
        7: "Part 7: World Indices Momentum"
    }
    st.markdown(f"<h2 style='text-align: center;'>{titles.get(st.session_state.current_page)}</h2>", unsafe_allow_html=True)

with col_next:
    if st.session_state.current_page < 7: st.button("Next ➡️", on_click=change_page, args=(st.session_state.current_page + 1,))

st.divider() 

# =======================================================
# PAGE 1: MARKET WATCHLIST
# =======================================================
if st.session_state.current_page == 1:
    df_watchlist = load_data_watchlist()
    
    if df_watchlist.empty:
        st.warning("⚠️ No data available. Please run data_fetcher.py locally to generate Parquet files.")
    else:
        st.sidebar.header("Filter & Rank Engine")
        sort_options = ["Market Cap (Cr)", "1D Return (%)", "1W Return (%)", "1M Return (%)", "3M Return (%)", "6M Return (%)", "1Y Return (%)", "Price", "Sector"]
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
        
        # Search Bar Left, Count Right
        col_search, col_count = st.columns(2)
        with col_search:
            search_query = st.text_input("🔍 Search Stock Symbol", placeholder="e.g. RELIANCE", label_visibility="collapsed").strip().upper()
            
        if search_query: df_filtered = df_filtered[df_filtered["Stock"].str.contains(search_query, case=False, na=False)]
        df_sorted = df_filtered.sort_values(by=sort_by, ascending=ascending).reset_index(drop=True)
        df_sorted.index = df_sorted.index + 1
        filtered_total = len(df_sorted)
        percentage = (filtered_total / base_total * 100) if base_total > 0 else 0

        with col_count:
            st.markdown(f"<div style='text-align: right; font-size: 16px; font-weight: bold; padding-top: 5px; padding-bottom: 10px;'>Matches: <span style='color: #4CAF50;'>{filtered_total}</span> / {base_total} (<span style='color: #4CAF50;'>{percentage:.1f}%</span>) of base</div>", unsafe_allow_html=True)

        st.dataframe(df_sorted.style.format(formatter={"Market Cap (Cr)": "{:,.0f}", "Price": "{:,.2f}", "1D Return (%)": "{:.2f}%", "1W Return (%)": "{:.2f}%", "1M Return (%)": "{:.2f}%", "3M Return (%)": "{:.2f}%", "6M Return (%)": "{:.2f}%", "1Y Return (%)": "{:.2f}%"}, na_rep="-"), use_container_width=True, height=600)

# =======================================================
# PAGE 2: BROAD INDICES
# =======================================================
elif st.session_state.current_page == 2:
    with st.spinner("Processing Broad Indices Data..."):
        df_indices, df_history = sync_and_get_metrics("nifty_indices_master.parquet", broad_index_config)
    
    if not df_indices.empty:
        timeframes = ["1D Return (%)", "1W Return (%)", "1M Return (%)", "3M Return (%)", "6M Return (%)", "1Y Return (%)"]
        st.markdown("**Select Timeframe:**")
        selected_timeframe = st.radio("Timeframe", timeframes, index=1, horizontal=True, label_visibility="collapsed")
        
        df_sorted_indices = df_indices.sort_values(by=selected_timeframe, ascending=False).reset_index(drop=True)
        df_sorted_indices['Color'] = np.where(df_sorted_indices[selected_timeframe] >= 0, 'Positive', 'Negative')
        df_sorted_indices['Label'] = df_sorted_indices[selected_timeframe].apply(lambda x: f"{x:.2f}%" if not pd.isna(x) else "")
        
        lookback_map = {"1D Return (%)": 2, "1W Return (%)": 6, "1M Return (%)": 22, "3M Return (%)": 64, "6M Return (%)": 126, "1Y Return (%)": len(df_history)}
        df_history.index.name = 'Date'
        df_slice = df_history.tail(lookback_map.get(selected_timeframe, 22))

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Index Returns for {selected_timeframe}**")
            fig_bar = px.bar(df_sorted_indices, x="Market Index", y=selected_timeframe, text="Label", color="Color", color_discrete_map={"Positive": "#00C853", "Negative": "#FF1744"})
            fig_bar.update_traces(textposition='outside')
            fig_bar.update_layout(showlegend=False, xaxis_title="", yaxis_title="Return (%)", height=400)
            st.plotly_chart(fig_bar, use_container_width=True)
            
        with col2:
            st.markdown(f"**Comparative Performance Waveform ({selected_timeframe})**")
            selected_lines = st.multiselect("Select indices to compare:", options=df_history.columns, default=list(df_history.columns))
            if selected_lines:
                df_slice_filtered = df_slice[selected_lines]
                df_normalized = (df_slice_filtered / df_slice_filtered.iloc[0] - 1) * 100
                df_melted = df_normalized.reset_index().melt(id_vars='Date', var_name='Index', value_name='Return (%)')
                fig_line = px.line(df_melted, x='Date', y='Return (%)', color='Index')
                fig_line.update_layout(xaxis_title="", yaxis_title="Cumulative Return (%)", height=400, legend_title_text="")
                st.plotly_chart(fig_line, use_container_width=True)
        
        st.divider()
        render_momentum_matrix(df_indices, "Broad Indices Momentum Matrix")
    else: st.warning("⚠️ Could not load data.")

# =======================================================
# PAGE 3: SECTORAL PULSE & DEEP DIVE (RESTORED TO V4)
# =======================================================
elif st.session_state.current_page == 3:
    with st.spinner("Processing Sectoral Data..."):
        df_sectors, _ = sync_and_get_metrics("nifty_indices_master.parquet", sectoral_config)
    
    if not df_sectors.empty:
        timeframes_sector = ["1D Return (%)", "1W Return (%)", "1M Return (%)", "3M Return (%)", "6M Return (%)", "1Y Return (%)"]
        st.markdown("**Select Momentum Timeframe:**")
        selected_timeframe_sector = st.radio("Timeframe", timeframes_sector, index=1, horizontal=True, label_visibility="collapsed")
        
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
        df_w = load_data_watchlist() 
        
        if constituent_symbols and not df_w.empty:
            df_drilled = df_w[df_w['Stock'].isin(constituent_symbols)].copy()
            if not df_drilled.empty:
                return_cols = ["1D Return (%)", "1W Return (%)", "1M Return (%)", "3M Return (%)", "6M Return (%)", "1Y Return (%)"]
                sector_stats = {col: {'mean': df_drilled[col].mean(), 'std': df_drilled[col].std()} for col in return_cols}
                
                col_empty, col_filters = st.columns([4, 1]) 
                with col_filters:
                    st.markdown("**Technical Filters:**")
                    dd_filter_50 = st.checkbox("Above 50 DMA ? (Yes/No)")
                    dd_filter_200 = st.checkbox("Above 200 DMA ? (Yes/No)")
                
                if dd_filter_50: df_drilled = df_drilled[df_drilled["Above 50 DMA?"] == "Yes"]
                if dd_filter_200: df_drilled = df_drilled[df_drilled["Above 200 DMA?"] == "Yes"]
                
                df_drilled_sorted = df_drilled.sort_values(by="Market Cap (Cr)", ascending=False).reset_index(drop=True)
                df_drilled_sorted.index = df_drilled_sorted.index + 1
                st.markdown(f"Showing **{len(df_drilled_sorted)}** stocks from the **Nifty {drill_sector}** index.")
                
                def apply_z_colors(s):
                    if s.name in return_cols:
                        mean, std = sector_stats[s.name]['mean'], sector_stats[s.name]['std']
                        colors = []
                        for val in s:
                            if pd.isna(val) or std == 0 or pd.isna(std): colors.append(''); continue
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
                
                st.dataframe(df_drilled_sorted.style.apply(apply_z_colors, axis=0).format(formatter={"Market Cap (Cr)": "{:,.0f}", "Price": "{:,.2f}", "1D Return (%)": "{:.2f}%", "1W Return (%)": "{:.2f}%", "1M Return (%)": "{:.2f}%", "3M Return (%)": "{:.2f}%", "6M Return (%)": "{:.2f}%", "1Y Return (%)": "{:.2f}%"}, na_rep="-"), use_container_width=True, height=600)
            else: st.info(f"The stocks in {drill_sector} are not currently in your Watchlist.")
    else: st.warning("⚠️ Could not load data for Nifty Sectoral Indices.")

# =======================================================
# PAGE 4: SECTORAL MOMENTUM MATRIX
# =======================================================
elif st.session_state.current_page == 4:
    with st.spinner("Calculating Sectoral Rankings..."):
        df_sectors, _ = sync_and_get_metrics("nifty_indices_master.parquet", sectoral_config)
        
    if not df_sectors.empty:
        render_momentum_matrix(df_sectors, "Nifty Sectoral Momentum Ranking Matrix")
    else:
        st.warning("⚠️ Could not load data for Nifty Sectoral Indices.")

# =======================================================
# PAGE 5: PRICE-VOLUME ACTION SCREENER
# =======================================================
elif st.session_state.current_page == 5:
    st.sidebar.header("⚡ Fast Intraday Sync")
    if st.sidebar.button("🔄 Force Refresh Data", use_container_width=True):
        def manual_sync():
            try:
                ed = pd.read_parquet("nifty_750_master.parquet")
                ed.index = pd.to_datetime(ed.index).tz_localize(None)
                tk = list(ed.columns.levels[0])
            except: return False
            nd = []
            for t in tk:
                try:
                    df = yf.Ticker(t).history(period="1d")
                    if not df.empty:
                        df.index = pd.to_datetime(df.index).tz_localize(None)
                        df.columns = pd.MultiIndex.from_product([[t], df.columns])
                        nd.append(df)
                except: pass
            if nd:
                pd.concat([ed, pd.concat(nd, axis=1)]).drop_duplicates(keep='last').to_parquet("nifty_750_master.parquet", engine="pyarrow")
                return True
            return False
            
        with st.spinner("Executing fast sync..."):
            if manual_sync(): st.sidebar.success("✅ Sync complete!"); st.cache_data.clear(); time.sleep(1); st.rerun()
            else: st.sidebar.error("❌ Sync failed.")

    st.sidebar.divider()
    st.sidebar.header("Price-Volume Parameters")
    
    @st.cache_data
    def get_pv_history():
        tickers, data = fetch_market_data_bulk()
        if data.empty: return {}
        s_dict = {}
        try:
            for _, r in pd.read_csv(GOOGLE_SHEET_CSV_URL).iterrows():
                s_dict[str(r.get('Stock Name', '')).strip()] = str(r.get('Sector', 'Unknown')).strip()
        except: pass
        h_recs = {}
        for t in tickers:
            try:
                df = data[t].dropna() if len(tickers) > 1 else data.dropna()
                if len(df) < 25: continue
                c, v = df['Close'], df['Volume']
                h_recs[t.replace('.NS', '')] = {
                    'Sector': s_dict.get(t.replace('.NS', ''), 'Unknown'),
                    'History': pd.DataFrame({'Close': c, 'Volume': v, 'Avg_Volume': v.rolling(20).mean(), 'Pct_Change': (c / c.shift(1) - 1) * 100, 'Vol_Surge': v / v.rolling(20).mean()}).tail(30)
                }
            except: pass
        return h_recs

    history_records = get_pv_history()
    
    if not history_records:
        st.warning("⚠️ Market data is empty.")
    else:
        all_dates = set()
        for info in history_records.values(): all_dates.update(info['History'].index.strftime('%Y-%m-%d').tolist())
        selected_date_str = st.sidebar.selectbox("Select Target Date:", sorted(list(all_dates), reverse=True))
        
        vol_multiplier = st.sidebar.number_input("Minimum Volume Surge (x Usual)", min_value=1.0, value=2.0, step=0.5)
        price_surge_pct = st.sidebar.number_input("Minimum Price Surge (%)", min_value=0.5, value=3.0, step=0.5)
        
        results = []
        for stock, info in history_records.items():
            df_hist_str = info['History'].copy()
            df_hist_str.index = df_hist_str.index.strftime('%Y-%m-%d')
            if selected_date_str in df_hist_str.index:
                row = df_hist_str.loc[selected_date_str]
                if row['Pct_Change'] >= price_surge_pct and row['Vol_Surge'] >= vol_multiplier:
                    ac = df_hist_str[(df_hist_str['Pct_Change'] >= price_surge_pct) & (df_hist_str['Vol_Surge'] >= vol_multiplier)]
                    results.append({"Stock": stock, "Sector": info['Sector'], "Close Price": row['Close'], "Volume": row['Volume'], "20D Avg Volume": row['Avg_Volume'], "Volume Surge (x)": row['Vol_Surge'], "Price Change (%)": row['Pct_Change'], "Action Count (30 Days)": f"{len(ac)} ({', '.join(ac.index.tolist())})"})
                    
        if results:
            df_res = pd.DataFrame(results).sort_values(by="Volume Surge (x)", ascending=False).reset_index(drop=True)
            df_res.index = df_res.index + 1
            st.markdown(f"### Stocks showing Price-Volume Action on **{selected_date_str}**")
            st.dataframe(df_res.style.format({"Close Price": "{:,.2f}", "Volume": "{:,.0f}", "20D Avg Volume": "{:,.0f}", "Volume Surge (x)": "{:.2f}x", "Price Change (%)": "{:.2f}%"}), use_container_width=True)
        else: st.info("No stocks met the criteria.")

# =======================================================
# PAGE 6: MACRO INDICATORS
# =======================================================
elif st.session_state.current_page == 6:
    macro_config = {"DX-Y.NYB": "Dollar Index", "GC=F": "Gold", "CL=F": "Crude Oil", "^TNX": "US 10 Yr Bond Yield", "INR=X": "USD INR", "^NSEI": "Nifty"}
    
    with st.spinner("Fetching Global Macro Data..."):
        _, df_history = sync_and_get_metrics("macro_master.parquet", macro_config)
        
    if not df_history.empty:
        if 'Gold' in df_history.columns and 'USD INR' in df_history.columns and 'Nifty' in df_history.columns:
            df_history['Gold to Nifty'] = (df_history['Gold'] * df_history['USD INR'] * 1.1 / 31.1) / df_history['Nifty']
            
        timeframes = ["1W", "1M", "3M", "6M", "1Y"]
        selected_timeframe = st.radio("Select Timeframe:", timeframes, index=4, horizontal=True)
        lookback_map = {"1W": 6, "1M": 22, "3M": 64, "6M": 126, "1Y": len(df_history)}
        df_history.index.name = 'Date'
        df_plot = df_history.tail(lookback_map.get(selected_timeframe, len(df_history))).reset_index()
        
        def render_macro_chart(title, y_col, note_text, color_hex, decimal_places=2):
            if y_col in df_history.columns:
                current_val = df_history[y_col].dropna().iloc[-1]
                st.markdown(f"#### {title} : `{current_val:,.{decimal_places}f}`")
                fig = px.line(df_plot, x='Date', y=y_col)
                fig.update_traces(line_color=color_hex, line_width=2)
                fig.update_layout(xaxis_title="", yaxis_title="", height=250, margin=dict(l=0, r=0, t=10, b=10))
                st.plotly_chart(fig, use_container_width=True)
                st.markdown(f"💡 *{note_text}*")
                st.divider()

        col1, col2 = st.columns(2)
        with col1:
            render_macro_chart("Dollar Index (DX-Y.NYB)", "Dollar Index", "When DX-Y increases, Gold decreases and Emerging Market decreases. Check for 100 mark.", "#1E88E5")
            render_macro_chart("Crude Oil in USD (CL=F)", "Crude Oil", "Higher crude puts pressure on import bills and inflation.", "#43A047")
        with col2:
            render_macro_chart("Gold in USD (GC=F)", "Gold", "Global Safe Haven Asset.", "#FFB300")
            render_macro_chart("US 10 Yr Bond Yield (^TNX)", "US 10 Yr Bond Yield", "For more FIIs the gap between Indian Bond and US Bond should be 5-7%.", "#D81B60", 3)
            
        render_macro_chart("Gold to Nifty Ratio", "Gold to Nifty", "0.6 above go to Stocks, 0.28 below go to Gold.", "#8E24AA", 4)
            
    else: st.warning("⚠️ Could not load Macro data. Ensure data_fetcher.py has generated macro_master.parquet.")

# =======================================================
# PAGE 7: WORLD INDICES MOMENTUM MATRIX
# =======================================================
elif st.session_state.current_page == 7:
    with st.spinner("Processing World Indices Momentum..."):
        df_world, _ = sync_and_get_metrics("world_indices_master.parquet", world_index_config)
    
    if not df_world.empty:
        render_momentum_matrix(df_world, "World Indices Momentum Ranking Matrix")
    else:
        st.warning("⚠️ Could not load data. Ensure data_fetcher.py has generated world_indices_master.parquet.")
