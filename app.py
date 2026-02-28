# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import io
import re
from supabase import create_client, Client

# --- Supabase Connection Configuration ---
SUPABASE_URL = "https://tqpwktjctngqtyisqtma.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRxcHdrdGpjdG5ncXR5aXNxdG1hIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjcyNjA2MTMsImV4cCI6MjA4MjgzNjYxM30.waTJLaeDz6k1xWhuSJQnW4nqjul6ZDOgnWK0MKrlSp4"

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    st.error(f"Failed to connect to Supabase: {e}")
    supabase = None

# --- Helper Functions ---
def create_download_button(df: pd.DataFrame, filename: str, button_key: str):
    """Create a download button for the processed DataFrame as an Excel file"""
    output_buffer = io.BytesIO()
    with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Processed_Data')
    st.download_button(
        label="📥 Download Processed File",
        data=output_buffer.getvalue(),
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=button_key
    )

# --- Core Processing Functions ---
def extract_mcode(sku_name):
    """
    Extract mcode from SKU Name:
    - If there are parentheses, extract the content inside the parentheses as mcode.
    - If there are no parentheses, use the entire string as mcode.
    """
    if not isinstance(sku_name, str) or not sku_name.strip():
        return None
    
    sku_name_str = str(sku_name)
    
    match = re.search(r'\((.*?)\)', sku_name_str)
    
    if match:
        return match.group(1).strip()
    else:
        return sku_name_str.strip()

def process_bigseller_file(uploaded_file):
    """Process BigSeller uploaded Excel file and update Count from Supabase"""
    if supabase is None:
        st.error("Unable to process file: Supabase connection failed.")
        return None

    st.info("1. Reading Excel file...")
    df = pd.read_excel(uploaded_file, dtype=str)

    st.info("2. Extracting mcode from 'SKU Name'...")
    df['mcode'] = df['SKU Name'].apply(extract_mcode)
    
    mcode_list = df['mcode'].dropna().unique().tolist()
    
    if not mcode_list:
        st.warning("No valid mcode found in the uploaded file.")
        return df.drop(columns=['mcode'])

    st.info(f"3. Querying Supabase for {len(mcode_list)} mcode entries...")
    try:
        response = supabase.table('server').select('mcode, stock_count, date').in_('mcode', mcode_list).execute()
        supabase_data = response.data
    except Exception as e:
        st.error(f"Error querying Supabase: {e}")
        return None

    stock_map = {str(item['mcode']).strip(): item['stock_count'] for item in supabase_data}
    st.success(f"Successfully retrieved {len(stock_map)} inventory records from Supabase.")
    
    # Extract and display the latest date from all records
    if supabase_data:
        dates = [item.get('date') for item in supabase_data if item.get('date')]
        if dates:
            try:
                dates_parsed = pd.to_datetime(dates)
                latest_date = dates_parsed.max()
                st.info(f"Last updated: {latest_date.strftime('%Y-%m-%d %H:%M:%S')}")
            except Exception:
                dates_sorted = sorted([str(d) for d in dates])
                if dates_sorted:
                    st.info(f"Last updated: {dates_sorted[-1]}")


    st.info("4. Updating 'Count' column...")
    
    def get_stock_count(row):
        mcode = row['mcode']
        if pd.notna(mcode) and mcode in stock_map:
            stock = stock_map[mcode]
            # Convert to float first to handle decimal types, then to int (truncates decimal part)
            stock_int = int(float(stock))
            return max(stock_int, 0)
        return 0

    df['Count'] = df.apply(get_stock_count, axis=1)

    st.success("File processing completed!")
    
    found_mcodes = set(stock_map.keys())
    all_mcodes = set(mcode_list)
    not_found_mcodes = list(all_mcodes - found_mcodes)

    if not_found_mcodes:
        with st.expander(f"⚠️ Warning: {len(not_found_mcodes)} mcode entries not found in Supabase (their Count has been set to 0). Click to view list."):
            st.warning("The following mcode entries have no matching records in Supabase 'server' table:")
            st.json(not_found_mcodes)
    else:
        st.success("Good news! All valid mcode entries found matching inventory records in Supabase.")
            
    # Remove specified columns from output
    columns_to_drop = ['Shelf Type', 'Area', 'Image URL']
    df = df.drop(columns=columns_to_drop, errors='ignore')
    st.info(f"Removed columns from output file: {', '.join(columns_to_drop)}")

    # Remove temporary mcode column
    df = df.drop(columns=['mcode'])
    return df

# --- Helper Function to Get Latest Stock Update Date ---
@st.cache_data(ttl=300)
def get_latest_stock_update_date():
    """Fetch the latest stock update date from Supabase"""
    if supabase is None:
        return None
    
    try:
        # Fetch records where date is not null, ordered by date descending
        response = supabase.table('server').select('date').not_.is_('date', 'null').order('date', desc=True).limit(1).execute()
        if response.data and len(response.data) > 0:
            latest_date_str = response.data[0].get('date')
            if latest_date_str:
                try:
                    latest_date = pd.to_datetime(latest_date_str)
                    return latest_date.strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    return str(latest_date_str)
    except Exception as e:
        pass
    
    return None

# --- Shopee Processing Function (Placeholder) ---
def process_shopee_file(uploaded_file):
    """Process Shopee uploaded Excel file (placeholder)"""
    st.info("Reading Shopee file...")
    df_input = pd.read_excel(uploaded_file)
    st.warning("Note: Shopee automation logic is not yet implemented.")
    st.write("Input Data Preview:")
    st.dataframe(df_input.head())
    df_output = df_input.copy()
    st.success("Shopee file processing complete (placeholder).")
    return df_output

# --- Streamlit UI Layout ---
st.set_page_config(page_title="Automated File Processor", layout="wide")
st.title("📈 Automated File Processor")
st.caption("Upload an Excel file, and the system will process it and generate a result file for you to download.")
tab1, tab2 = st.tabs(["BigSeller", "Shopee"])
with tab1:
    st.header("BigSeller Data Processing")
    
    # Display the latest stock update date
    latest_date = get_latest_stock_update_date()
    if latest_date:
        st.info(f"Last stock data updated: {latest_date}")
    else:
        st.warning("Unable to fetch latest stock update date. Please check if the 'date' column exists in Supabase.")
    
    uploaded_file_bs = st.file_uploader("Please upload the BigSeller Excel file", type=["xlsx", "xls"], key="bigseller_uploader")
    if uploaded_file_bs is not None:
        if st.button("🚀 Process BigSeller File", key="bigseller_process"):
            with st.spinner('Processing in progress, please wait...'):
                result_df_bs = process_bigseller_file(uploaded_file_bs)
                if result_df_bs is not None:
                    create_download_button(df=result_df_bs, filename="output_bigseller.xlsx", button_key="bigseller_downloader")
with tab2:
    st.header("Shopee Data Processing")
    uploaded_file_shopee = st.file_uploader("Please upload the Shopee Excel file", type=["xlsx", "xls"], key="shopee_uploader")
    if uploaded_file_shopee is not None:
        if st.button("🚀 Process Shopee File", key="shopee_process"):
            with st.spinner('Processing in progress...'):
                result_df_shopee = process_shopee_file(uploaded_file_shopee)
                if result_df_shopee is not None:
                    create_download_button(df=result_df_shopee, filename="output_shopee.xlsx", button_key="shopee_downloader")
