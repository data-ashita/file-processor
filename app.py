# -*- coding: utf-8 -*-
import streamlit as st
import pytz
import pandas as pd
import io
import re
import time
import json
import os
from supabase import create_client, Client
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload

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
def get_latest_stock_update_date(max_retries=3, initial_delay=1):
    """Fetch the latest stock update date from Supabase with retry logic"""
    if supabase is None:
        return None
    
    retry_count = 0
    delay = initial_delay
    
    while retry_count < max_retries:
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
            # If no data but no error, return None
            return None
        except Exception as e:
            retry_count += 1
            if retry_count < max_retries:
                # Exponential backoff: wait before retrying
                time.sleep(delay)
                delay *= 2  # Double the delay for next retry
            else:
                # All retries exhausted
                pass
    
    return None

# --- Google Drive Integration ---
def get_google_drive_service():
    """Initialize Google Drive service using service account credentials from Streamlit Secrets"""
    try:
        # Read service account credentials from Streamlit Secrets
        if "google" in st.secrets:
            credentials_dict = dict(st.secrets["google"])
            credentials = service_account.Credentials.from_service_account_info(
                credentials_dict,
                scopes=['https://www.googleapis.com/auth/drive']
            )
            service = build('drive', 'v3', credentials=credentials)
            return service
        else:
            st.error("Google credentials not found in Streamlit Secrets")
    except Exception as e:
        st.error(f"Error connecting to Google Drive: {e}")
    return None

def get_file_status_and_date(service, file_id):
    """Get file status and last modified date from Google Drive in GMT+8"""
    try:
        file = service.files().get(fileId=file_id, fields='id, name, modifiedTime, trashed').execute()
        
        if file.get('trashed'):
            return 'error', None
        
        modified_time = file.get('modifiedTime')
        if modified_time:
            # Convert ISO format to GMT+8
            from datetime import datetime
            dt = datetime.fromisoformat(modified_time.replace('Z', '+00:00'))
            
            # Convert to GMT+8
            gmt8 = pytz.timezone('Asia/Shanghai')
            dt_gmt8 = dt.astimezone(gmt8)
            
            return 'valid', dt_gmt8.strftime('%Y-%m-%d %H:%M:%S')
        
        return 'valid', 'Unknown'
    except Exception as e:
        return 'error', None

def update_file_by_id(service, file_id, file_content):
    """Update a file in Google Drive by ID (overwrites content, keeps ID)"""
    try:
        media = MediaIoBaseUpload(file_content, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        file = service.files().update(fileId=file_id, media_body=media, fields='id, modifiedTime').execute()
        return True, file.get('id'), file.get('modifiedTime')
    except Exception as e:
        st.error(f"Error updating file: {e}")
    return False, None, None

# --- Google Drive Integration ---
def get_google_drive_service():
    """Initialize Google Drive service using service account credentials from Streamlit Secrets"""
    try:
        # Read service account credentials from Streamlit Secrets
        if "google" in st.secrets:
            credentials_dict = dict(st.secrets["google"])
            credentials = service_account.Credentials.from_service_account_info(
                credentials_dict,
                scopes=['https://www.googleapis.com/auth/drive']
            )
            service = build('drive', 'v3', credentials=credentials)
            return service
        else:
            st.error("Google credentials not found in Streamlit Secrets")
    except Exception as e:
        st.error(f"Error connecting to Google Drive: {e}")
    return None

def get_file_status_and_date(service, file_id):
    """Get file status and last modified date from Google Drive in GMT+8"""
    try:
        file = service.files().get(fileId=file_id, fields='id, name, modifiedTime, trashed').execute()
        
        if file.get('trashed'):
            return 'error', None
        
        modified_time = file.get('modifiedTime')
        if modified_time:
            # Convert ISO format to GMT+8
            from datetime import datetime
            dt = datetime.fromisoformat(modified_time.replace('Z', '+00:00'))
            
            # Convert to GMT+8
            gmt8 = pytz.timezone('Asia/Shanghai')
            dt_gmt8 = dt.astimezone(gmt8)
            
            return 'valid', dt_gmt8.strftime('%Y-%m-%d %H:%M:%S')
        
        return 'valid', 'Unknown'
    except Exception as e:
        return 'error', None

def update_file_by_id(service, file_id, file_content):
    """Update a file in Google Drive by ID (overwrites content, keeps ID)"""
    try:
        media = MediaIoBaseUpload(file_content, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        file = service.files().update(fileId=file_id, media_body=media, fields='id, modifiedTime').execute()
        return True, file.get('id'), file.get('modifiedTime')
    except Exception as e:
        st.error(f"Error updating file: {e}")
    return False, None, None

# --- PO Processing Helper Functions ---
def extract_product_id_from_sn(sn):
    """Extract the first 5 digits from SN as Product ID"""
    if not isinstance(sn, str) or not sn.strip():
        return None
    sn_str = str(sn).strip()
    match = re.search(r'^(\d{5})', sn_str)
    return match.group(1) if match else None

def get_stock_code_from_masterfile(goods_name):
    """Get ashita_stock_code from stock_imei_mapping using goods_name"""
    if supabase is None or not goods_name:
        return None
    try:
        response = supabase.table('ashita_stock_code').select('ashita_stock_code').eq('goods_name', str(goods_name)).execute()
        if response.data and len(response.data) > 0:
            return response.data[0].get('ashita_stock_code')
    except Exception:
        pass
    return None

def get_ean_from_xiaomi_price_structure(product_id):
    """Get EAN from xiaomi_price_structure using Product ID"""
    if supabase is None or not product_id:
        return None
    try:
        response = supabase.table('xiaomi_price_structure').select('EAN').eq('Product ID', str(product_id)).execute()
        if response.data and len(response.data) > 0:
            return response.data[0].get('EAN')
    except Exception:
        pass
    return None

def add_to_stock_imei_mapping(goods_name, stock_code):
    """Add or update record in stock_imei_mapping"""
    if supabase is None or not goods_name or not stock_code:
        return False
    try:
        response = supabase.table('ashita_stock_code').select('ashita_stock_code').eq('goods_name', str(goods_name)).execute()
        if response.data and len(response.data) > 0:
            supabase.table('ashita_stock_code').update({'ashita_stock_code': str(stock_code)}).eq('goods_name', str(goods_name)).execute()
        else:
            supabase.table('ashita_stock_code').insert({'goods_name': str(goods_name), 'ashita_stock_code': str(stock_code)}).execute()
        return True
    except Exception:
        return False

def extract_batch_number(related_doc):
    """Extract batch number starting with IOT or PH from Related Document"""
    if not isinstance(related_doc, str) or not related_doc.strip():
        return None
    related_doc_str = str(related_doc).strip()
    match = re.search(r'(IOT|PH)[\w_]*', related_doc_str)
    return match.group(0) if match else None

def generate_do_number(batch_number, sequence_number):
    """Generate DO Number from batch number with sequence, ensuring max 10 characters
    
    Rules:
    - If starts with IOT: keep original format, add sequence suffix
      Example: IOT2602240-1
    - If starts with PH: extract only digits, add sequence suffix
      Example: PH2602121_MY-3 -> 2602121-3
      If digits exceed 10 chars total, trim from the front
      Example: PH1234567890_MY-1 -> 34567890-1
    """
    if not batch_number:
        return None
    
    suffix = f"-{sequence_number}"
    suffix_len = len(suffix)
    
    if batch_number.startswith('IOT'):
        # For IOT: keep original format
        prefix = 'IOT'
        middle = batch_number[3:]
        max_middle_length = 10 - len(prefix) - suffix_len
        if len(middle) > max_middle_length:
            middle = middle[-max_middle_length:]
        do_number = prefix + middle + suffix
    elif batch_number.startswith('PH'):
        # For PH: extract only digits
        digits = re.sub(r'\D', '', batch_number)  # Remove all non-digit characters
        max_digits_length = 10 - suffix_len
        if len(digits) > max_digits_length:
            digits = digits[-max_digits_length:]  # Keep rightmost digits
        do_number = digits + suffix
    else:
        # Fallback for other prefixes
        prefix = batch_number[:2]
        middle = batch_number[2:]
        max_middle_length = 10 - len(prefix) - suffix_len
        if len(middle) > max_middle_length:
            middle = middle[-max_middle_length:]
        do_number = prefix + middle + suffix
    
    return do_number if len(do_number) <= 10 else None

def get_imei_mapping_data(stock_codes):
    """Batch query imei_mapping table using ashita_stock_code"""
    if supabase is None or not stock_codes:
        return {}
    try:
        response = supabase.table('imei_mapping').select('ashita_stock_code, batch_control, mapped_supplier').in_('ashita_stock_code', stock_codes).execute()
        imei_map = {}
        for item in response.data:
            stock_code = item.get('ashita_stock_code')
            if stock_code:
                imei_map[str(stock_code)] = {
                    'batch_control': item.get('batch_control', False),
                    'mapped_supplier': item.get('mapped_supplier')
                }
        return imei_map
    except Exception as e:
        st.warning(f"Error querying imei_mapping: {e}")
        return {}

def update_batch_control(goods_name, new_value=True):
    """Update batch_control status in imei_mapping"""
    if supabase is None or not goods_name:
        return False
    try:
        supabase.table('imei_mapping').update({'batch_control': new_value}).eq('goods_name', str(goods_name)).execute()
        return True
    except Exception:
        return False

def process_imei_logic(df, imei_mapping_data):
    """
    Process IMEI based on five cases using Stock Code lookup:
    1. Stock Code exists in imei_mapping, batch_control=true, mapped_supplier has code, has IMEI -> use Excel IMEI
    2. Stock Code exists in imei_mapping, batch_control=true, mapped_supplier has code, no IMEI -> mark for scan
    3. Stock Code exists in imei_mapping, batch_control=false, mapped_supplier is empty, has IMEI -> use Excel IMEI and mark for adjustment
    4. Stock Code exists in imei_mapping, batch_control=false, mapped_supplier is empty, no IMEI -> skip
    5. Stock Code not found in imei_mapping -> error
    
    Returns: (processed_df, error_rows, scan_needed_list, adjustment_needed_list)
    """
    df['IMEI'] = df['IMEI'].fillna('')
    df['IMEI Status'] = ''
    
    error_rows = []
    scan_needed = set()
    adjustment_needed = set()
    
    for idx, row in df.iterrows():
        stock_code = str(row.get('Stock Code', '')).strip() if pd.notna(row.get('Stock Code')) else ''
        excel_imei = row.get('IMEI', '').strip()
        goods_name = row.get('Goods name', '').strip()
        
        # If no Stock Code, mark as error
        if not stock_code:
            df.at[idx, 'IMEI Status'] = 'ERROR: No Stock Code found'
            error_rows.append(idx)
            continue
        
        # Case 5: Stock Code not found in imei_mapping
        if stock_code not in imei_mapping_data:
            df.at[idx, 'IMEI Status'] = 'ERROR: Stock Code not found in imei_mapping'
            error_rows.append(idx)
            continue
        
        mapping_info = imei_mapping_data[stock_code]
        batch_control = mapping_info.get('batch_control', False)
        mapped_supplier = mapping_info.get('mapped_supplier', '')
        has_supplier = bool(mapped_supplier and str(mapped_supplier).strip())
        
        # Case 1: batch_control=true, mapped_supplier has code, has IMEI
        if batch_control and has_supplier and excel_imei:
            df.at[idx, 'IMEI'] = excel_imei
            df.at[idx, 'IMEI Status'] = 'Case 1: Used Excel IMEI'
        
        # Case 2: batch_control=true, mapped_supplier has code, no IMEI
        elif batch_control and has_supplier and not excel_imei:
            df.at[idx, 'IMEI Status'] = 'Case 2: Needs manual scan'
            scan_needed.add(stock_code)
        
        # Case 3: batch_control=false, mapped_supplier is empty, has IMEI
        elif not batch_control and not has_supplier and excel_imei:
            df.at[idx, 'IMEI'] = excel_imei
            df.at[idx, 'IMEI Status'] = 'Case 3: Used Excel IMEI (needs adjustment)'
            adjustment_needed.add(stock_code)
        
        # Case 4: batch_control=false, mapped_supplier is empty, no IMEI
        elif not batch_control and not has_supplier and not excel_imei:
            df.at[idx, 'IMEI'] = ''
            df.at[idx, 'IMEI Status'] = 'Case 4: Skipped (no IMEI needed)'
        
        # Other cases - shouldn't happen based on requirements, but handle gracefully
        else:
            df.at[idx, 'IMEI Status'] = 'Not processed (unexpected condition)'
    
    return df, error_rows, list(scan_needed), list(adjustment_needed)

def get_imei_mapping_data(stock_codes):
    """Batch query imei_mapping table using ashita_stock_code"""
    if supabase is None or not stock_codes:
        return {}
    try:
        response = supabase.table('imei_mapping').select('ashita_stock_code, batch_control, mapped_supplier').in_('ashita_stock_code', stock_codes).execute()
        imei_map = {}
        for item in response.data:
            stock_code = item.get('ashita_stock_code')
            if stock_code:
                imei_map[str(stock_code)] = {
                    'batch_control': item.get('batch_control', False),
                    'mapped_supplier': item.get('mapped_supplier')
                }
        return imei_map
    except Exception as e:
        st.warning(f"Error querying imei_mapping: {e}")
        return {}

def update_batch_control(goods_name, new_value=True):
    """Update batch_control status in imei_mapping"""
    if supabase is None or not goods_name:
        return False
    try:
        supabase.table('imei_mapping').update({'batch_control': new_value}).eq('goods_name', str(goods_name)).execute()
        return True
    except Exception:
        return False

def process_imei_logic(df, imei_mapping_data):
    """
    Process IMEI based on five cases using Stock Code lookup:
    1. Stock Code exists in imei_mapping, batch_control=true, mapped_supplier has code, has IMEI -> use Excel IMEI
    2. Stock Code exists in imei_mapping, batch_control=true, mapped_supplier has code, no IMEI -> mark for scan
    3. Stock Code exists in imei_mapping, batch_control=false, mapped_supplier is empty, has IMEI -> use Excel IMEI and mark for adjustment
    4. Stock Code exists in imei_mapping, batch_control=false, mapped_supplier is empty, no IMEI -> skip
    5. Stock Code not found in imei_mapping -> error
    
    Returns: (processed_df, error_rows, scan_needed_list, adjustment_needed_list)
    """
    df['IMEI'] = df['IMEI'].fillna('')
    df['IMEI Status'] = ''
    
    error_rows = []
    scan_needed = set()
    adjustment_needed = set()
    
    for idx, row in df.iterrows():
        stock_code = str(row.get('Stock Code', '')).strip() if pd.notna(row.get('Stock Code')) else ''
        excel_imei = row.get('IMEI', '').strip()
        goods_name = row.get('Goods name', '').strip()
        
        # If no Stock Code, mark as error
        if not stock_code:
            df.at[idx, 'IMEI Status'] = 'ERROR: No Stock Code found'
            error_rows.append(idx)
            continue
        
        # Case 5: Stock Code not found in imei_mapping
        if stock_code not in imei_mapping_data:
            df.at[idx, 'IMEI Status'] = 'ERROR: Stock Code not found in imei_mapping'
            error_rows.append(idx)
            continue
        
        mapping_info = imei_mapping_data[stock_code]
        batch_control = mapping_info.get('batch_control', False)
        mapped_supplier = mapping_info.get('mapped_supplier', '')
        has_supplier = bool(mapped_supplier and str(mapped_supplier).strip())
        
        # Case 1: batch_control=true, mapped_supplier has code, has IMEI
        if batch_control and has_supplier and excel_imei:
            df.at[idx, 'IMEI'] = excel_imei
            df.at[idx, 'IMEI Status'] = 'Case 1: Used Excel IMEI'
        
        # Case 2: batch_control=true, mapped_supplier has code, no IMEI
        elif batch_control and has_supplier and not excel_imei:
            df.at[idx, 'IMEI Status'] = 'Case 2: Needs manual scan'
            scan_needed.add(stock_code)
        
        # Case 3: batch_control=false, mapped_supplier is empty, has IMEI
        elif not batch_control and not has_supplier and excel_imei:
            df.at[idx, 'IMEI'] = excel_imei
            df.at[idx, 'IMEI Status'] = 'Case 3: Used Excel IMEI (needs adjustment)'
            adjustment_needed.add(stock_code)
        
        # Case 4: batch_control=false, mapped_supplier is empty, no IMEI
        elif not batch_control and not has_supplier and not excel_imei:
            df.at[idx, 'IMEI'] = ''
            df.at[idx, 'IMEI Status'] = 'Case 4: Skipped (no IMEI needed)'
        
        # Other cases - shouldn't happen based on requirements, but handle gracefully
        else:
            df.at[idx, 'IMEI Status'] = 'Not processed (unexpected condition)'
    
    return df, error_rows, list(scan_needed), list(adjustment_needed)

def process_po_file(uploaded_file):
    """Process PO file with three-tier Stock Code extraction logic (optimized with batch queries)"""
    if supabase is None:
        st.error("Unable to process file: Supabase connection failed.")
        return None, None
    st.info("1. Reading Excel file...")
    df = pd.read_excel(uploaded_file, dtype=str)
    df['Stock Code'] = None
    df['Manual Input Required'] = False
    df['Processing Status'] = ''
    df['DO Number'] = None
    st.info("2. Extracting unique batch numbers and generating DO Numbers...")
    df['Batch Number'] = df['Related Document'].apply(extract_batch_number)
    unique_batches = df['Batch Number'].dropna().unique().tolist()
    batch_do_map = {}
    if unique_batches:
        for seq, batch in enumerate(unique_batches, 1):
            do_number = generate_do_number(batch, seq)
            batch_do_map[batch] = do_number
        for idx, row in df.iterrows():
            batch = row.get('Batch Number')
            if batch and batch in batch_do_map:
                df.at[idx, 'DO Number'] = batch_do_map[batch]
    st.info("3. Extracting unique goods names and product IDs...")
    unique_goods = df['Goods name'].dropna().unique().tolist()
    df['Product ID'] = df['SN'].apply(extract_product_id_from_sn)
    unique_product_ids = df['Product ID'].dropna().unique().tolist()
    st.info("4. Batch querying Supabase for stock_imei_mapping...")
    try:
        stock_imei_map = {}
        if unique_goods:
            response = supabase.table('ashita_stock_code').select('goods_name, ashita_stock_code').in_('goods_name', unique_goods).execute()
            for item in response.data:
                stock_imei_map[item['goods_name']] = item['ashita_stock_code']
    except Exception as e:
        st.warning(f"Error querying stock_imei_mapping: {e}")
        stock_imei_map = {}
    st.info("5. Batch querying Supabase for xiaomi_price_structure...")
    try:
        xiaomi_ean_map = {}
        if unique_product_ids:
            response = supabase.table('xiaomi_price_structure').select('*').in_('Product ID', unique_product_ids).execute()
            for item in response.data:
                product_id_value = item.get('Product ID')
                ean_value = item.get('EAN')
                if product_id_value and ean_value:
                    xiaomi_ean_map[str(product_id_value)] = ean_value
    except Exception as e:
        st.warning(f"Error querying xiaomi_price_structure: {e}")
        xiaomi_ean_map = {}
    st.info("6. Processing Stock Code extraction...")
    manual_input_needed = {}
    for idx, row in df.iterrows():
        goods_name = row.get('Goods name')
        product_id = row.get('Product ID')
        if not goods_name:
            df.at[idx, 'Processing Status'] = 'Missing Goods name'
            continue
        if goods_name in stock_imei_map:
            df.at[idx, 'Stock Code'] = stock_imei_map[goods_name]
            df.at[idx, 'Processing Status'] = 'Found in stock_imei_mapping'
            continue
        if product_id and product_id in xiaomi_ean_map:
            ean = xiaomi_ean_map[product_id]
            if add_to_stock_imei_mapping(goods_name, ean):
                df.at[idx, 'Stock Code'] = ean
                df.at[idx, 'Processing Status'] = 'Found via SN and added to stock_imei_mapping'
                stock_imei_map[goods_name] = ean
                continue
        df.at[idx, 'Manual Input Required'] = True
        df.at[idx, 'Processing Status'] = 'Manual input required'
        if goods_name not in manual_input_needed:
            manual_input_needed[goods_name] = idx
    df = df.drop(columns=['Product ID'])
    st.success(f"Stock Code extraction completed. {len(manual_input_needed)} entries require manual input.")
    
    st.info("7. Processing IMEI matching from imei_mapping table...")
    # Get all unique stock codes from the dataframe
    unique_stock_codes = df['Stock Code'].dropna().unique().tolist()
    unique_stock_codes = [str(code).strip() for code in unique_stock_codes if code]
    imei_mapping_data = get_imei_mapping_data(unique_stock_codes)
    df, error_rows, scan_needed, adjustment_needed = process_imei_logic(df, imei_mapping_data)
    
    # Prepare error file data
    error_file_data = {
        'error_rows': error_rows,
        'scan_needed': scan_needed,
        'adjustment_needed': adjustment_needed,
        'manual_input_needed': manual_input_needed
    }
    
    st.success(f"IMEI processing completed. Errors: {len(error_rows)}, Scan needed: {len(scan_needed)}, Adjustments: {len(adjustment_needed)}")
    return df, error_file_data

# --- Streamlit UI Layout ---
st.set_page_config(page_title="Automated File Processor", layout="wide")
st.title("📈 Automated File Processor")
st.caption("Upload an Excel file, and the system will process it and generate a result file for you to download.")
tab1, tab2, tab3 = st.tabs(["BigSeller", "PO", "Shopee"])
with tab1:
    st.header("BigSeller Data Processing")
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
    st.header("PO Data Processing")
    
    # 启用多文件上传
    uploaded_files_po = st.file_uploader(
        "Please upload PO Excel files (you can select multiple files)", 
        type=["xlsx", "xls"], 
        key="po_uploader",
        accept_multiple_files=True
    )
    
    if uploaded_files_po:
        st.info(f"📁 {len(uploaded_files_po)} file(s) selected for processing")
        
        if st.button("🚀 Process PO Files", key="po_process"):
            with st.spinner('Processing files in progress, please wait...'):
                all_results = []
                all_error_data_list = []
                
                progress_bar = st.progress(0)
                
                # 逐个处理每个文件
                for file_idx, uploaded_file in enumerate(uploaded_files_po, 1):
                    st.info(f"⏳ Processing file {file_idx}/{len(uploaded_files_po)}: {uploaded_file.name}")
                    
                    result_df, error_file_data = process_po_file(uploaded_file)
                    
                    if result_df is not None:
                        all_results.append(result_df)
                        all_error_data_list.append({
                            'file_name': uploaded_file.name,
                            'result_df': result_df,
                            'error_data': error_file_data
                        })
                        st.success(f"✅ File {file_idx} processed successfully")
                    else:
                        st.error(f"❌ File {file_idx} processing failed")
                    
                    progress_bar.progress(file_idx / len(uploaded_files_po))
                
                st.success(f"✅ All {len(uploaded_files_po)} files processed!")
                
                # ========== 关键改动：为每个文件分别输出 ==========
                if all_results:
                    st.divider()
                    st.subheader("📥 Download Results by File")
                    
                    # 为每个文件创建一个下载部分
                    for file_data in all_error_data_list:
                        file_name = file_data['file_name']
                        result_df = file_data['result_df']
                        error_file_data = file_data['error_data']
                        
                        # 去掉文件扩展名作为标识
                        file_base_name = file_name.rsplit('.', 1)[0]
                        
                        st.subheader(f"📄 {file_name}")
                        
                        # 分类结果
                        complete_df = result_df[
                            (result_df['Stock Code'].notna()) & 
                            (~result_df['IMEI Status'].str.contains('ERROR', na=False))
                        ].copy()
                        
                        incomplete_stock_df = result_df[
                            result_df['Stock Code'].isna()
                        ].copy()
                        
                        incomplete_imei_df = result_df[
                            (result_df['Stock Code'].notna()) & 
                            (result_df['IMEI Status'].str.contains('ERROR', na=False))
                        ].copy()
                        
                        col1, col2, col3, col4 = st.columns(4)
                        
                        # Complete 下载按钮
                        with col1:
                            st.metric("Complete Records", len(complete_df))
                            if len(complete_df) > 0:
                                output_complete = complete_df[['Stock Code', 'IMEI', 'DO Number', 'Medium Box Code']].copy()
                                txt_content = output_complete.to_csv(sep='\t', index=False, header=False)
                                st.download_button(
                                    label="📥 Download Complete", 
                                    data=txt_content, 
                                    file_name=f"output_{file_base_name}_complete.txt", 
                                    mime="text/plain", 
                                    key=f"po_complete_download_{file_base_name}"
                                )
                        
                        # Incomplete Stock Code 下载按钮
                        with col2:
                            st.metric("Incomplete Stock Code", len(incomplete_stock_df))
                            if len(incomplete_stock_df) > 0:
                                output_incomplete_stock = incomplete_stock_df[['Goods name', 'DO Number', 'Medium Box Code', 'SN', 'Processing Status']].copy()
                                output_buffer = io.BytesIO()
                                with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                                    output_incomplete_stock.to_excel(writer, index=False, sheet_name='Missing Stock Code')
                                st.download_button(
                                    label="📥 Download Stock Code", 
                                    data=output_buffer.getvalue(), 
                                    file_name=f"output_{file_base_name}_incomplete_stock.xlsx", 
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                                    key=f"po_incomplete_stock_download_{file_base_name}"
                                )
                        
                        # Incomplete IMEI 下载按钮
                        with col3:
                            st.metric("Incomplete IMEI", len(incomplete_imei_df))
                            if len(incomplete_imei_df) > 0:
                                output_incomplete_imei = incomplete_imei_df[['Stock Code', 'Goods name', 'IMEI', 'DO Number', 'IMEI Status']].copy()
                                output_buffer = io.BytesIO()
                                with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                                    output_incomplete_imei.to_excel(writer, index=False, sheet_name='IMEI Issues')
                                st.download_button(
                                    label="📥 Download IMEI Issues", 
                                    data=output_buffer.getvalue(), 
                                    file_name=f"output_{file_base_name}_incomplete_imei.xlsx", 
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                                    key=f"po_incomplete_imei_download_{file_base_name}"
                                )
                        
                        # Error/Action Items 下载按钮
                        with col4:
                            error_rows = error_file_data.get('error_rows', [])
                            scan_needed = error_file_data.get('scan_needed', [])
                            adjustment_needed = error_file_data.get('adjustment_needed', [])
                            error_count = len(error_rows) + len(scan_needed) + len(adjustment_needed)
                            
                            st.metric("Error/Action Items", error_count)
                            if error_count > 0:
                                error_sheet_data = []
                                
                                if error_rows:
                                    error_df = result_df.iloc[error_rows].copy()
                                    error_sheet_data.append(("Error Rows", error_df))
                                
                                if scan_needed:
                                    scan_df = pd.DataFrame({
                                        'Goods Name': scan_needed,
                                        'Action': ['Needs manual IMEI scan'] * len(scan_needed)
                                    })
                                    error_sheet_data.append(("Scan Needed", scan_df))
                                
                                if adjustment_needed:
                                    adjust_df = pd.DataFrame({
                                        'Goods Name': adjustment_needed,
                                        'Action': ['Update batch_control to true'] * len(adjustment_needed)
                                    })
                                    error_sheet_data.append(("Adjustment Needed", adjust_df))
                                
                                output_error = io.BytesIO()
                                with pd.ExcelWriter(output_error, engine='openpyxl') as writer:
                                    for sheet_name, df in error_sheet_data:
                                        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
                                
                                st.download_button(
                                    label="📥 Download Error/Action File", 
                                    data=output_error.getvalue(), 
                                    file_name=f"output_{file_base_name}_errors.xlsx", 
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                                    key=f"po_error_download_{file_base_name}"
                                )

# --- Shopee Tab ---
with tab3:
    st.header("Shopee Data Processing")
    
    # Google Drive configuration
    GOOGLE_DRIVE_FOLDER_ID = "1kfTp67K0xaHfLhbYJe_88GPSAid7A59I"
    FILE_MAPPING = {
        "media": {
            "name": "mass_update_media_info.xlsx",
            "id": "131TbApuNtVbw6cIqwNLsD3McWOEHA-Hz"
        },
        "shipping": {
            "name": "mass_update_shipping_info.xlsx",
            "id": "1bAebllOT1uj0mvv4_S1UBthFMO48U63w"
        },
        "sales": {
            "name": "mass_update_sales_info.xlsx",
            "id": "1f8T17KsG1dwAiFfH0Gek-go6X-ra3aTA"
        },
        "price": {
            "name": "iot_price_structure.xlsx",
            "id": "18xG8_m3IiYe4M4HHrN4I0QwhE4rkUp65"
        }
    }
    
    # Get Google Drive service for status checking
    service = get_google_drive_service()
    
    # Create three columns for upload buttons with status
    col_media, col_shipping, col_sales, col_price = st.columns(4)
    
    with col_media:
        st.subheader("📸 Media File")
        uploaded_file_media = st.file_uploader("Upload Media Excel file", type=["xlsx", "xls"], key="shopee_media_uploader")
        if uploaded_file_media is not None:
            st.success(f"✓ Media file uploaded: {uploaded_file_media.name}")
        
        # Display status and date
        if service:
            status, modified_date = get_file_status_and_date(service, FILE_MAPPING["media"]["id"])
            if status == 'valid':
                st.caption(f"✅ Valid | {modified_date}")
            else:
                st.caption(f"❌ Error")
    
    with col_shipping:
        st.subheader("🚚 Shipping File")
        uploaded_file_shipping = st.file_uploader("Upload Shipping Excel file", type=["xlsx", "xls"], key="shopee_shipping_uploader")
        if uploaded_file_shipping is not None:
            st.success(f"✓ Shipping file uploaded: {uploaded_file_shipping.name}")
        
        # Display status and date
        if service:
            status, modified_date = get_file_status_and_date(service, FILE_MAPPING["shipping"]["id"])
            if status == 'valid':
                st.caption(f"✅ Valid | {modified_date}")
            else:
                st.caption(f"❌ Error")
    
    with col_sales:
        st.subheader("💰 Sales File")
        uploaded_file_sales = st.file_uploader("Upload Sales Excel file", type=["xlsx", "xls"], key="shopee_sales_uploader")
        if uploaded_file_sales is not None:
            st.success(f"✓ Sales file uploaded: {uploaded_file_sales.name}")
        
        # Display status and date
        if service:
            status, modified_date = get_file_status_and_date(service, FILE_MAPPING["sales"]["id"])
            if status == 'valid':
                st.caption(f"✅ Valid | {modified_date}")
            else:
                st.caption(f"❌ Error")
    
    
    with col_price:
        st.subheader("💰 Price File")
        uploaded_file_price = st.file_uploader("Upload Price Excel file", type=["xlsx", "xls"], key="shopee_price_uploader")
        if uploaded_file_price is not None:
            st.success(f"✓ Price file uploaded: {uploaded_file_price.name}")
        
        # Display status and date
        if service:
            status, modified_date = get_file_status_and_date(service, FILE_MAPPING["price"]["id"])
            if status == 'valid':
                st.caption(f"✅ Valid | {modified_date}")
            else:
                st.caption(f"❌ Error")
    st.divider()
    
    # ========== 修复部分开始：添加 Price File 到上传条件 ==========
    # Process and upload button
    # 修改：添加 or uploaded_file_price is not None
    if uploaded_file_media is not None or uploaded_file_shipping is not None or uploaded_file_sales is not None or uploaded_file_price is not None:
        if st.button("🚀 Upload to Google Drive", key="shopee_upload"):
            with st.spinner("Uploading files to Google Drive..."):
                service = get_google_drive_service()
                
                if service is None:
                    st.error("❌ Failed to connect to Google Drive. Please check credentials.")
                else:
                    upload_results = {}
                    
                    # Upload Media file
                    if uploaded_file_media is not None:
                        file_content = io.BytesIO(uploaded_file_media.getvalue())
                        success, file_id, modified_time = update_file_by_id(service, FILE_MAPPING["media"]["id"], file_content)
                        upload_results["Media"] = success
                        if success:
                            st.success(f"✅ Media file uploaded successfully")
                        else:
                            st.error("❌ Failed to upload Media file")
                    
                    # Upload Shipping file
                    if uploaded_file_shipping is not None:
                        file_content = io.BytesIO(uploaded_file_shipping.getvalue())
                        success, file_id, modified_time = update_file_by_id(service, FILE_MAPPING["shipping"]["id"], file_content)
                        upload_results["Shipping"] = success
                        if success:
                            st.success(f"✅ Shipping file uploaded successfully")
                        else:
                            st.error("❌ Failed to upload Shipping file")
                    
                    # Upload Sales file
                    if uploaded_file_sales is not None:
                        file_content = io.BytesIO(uploaded_file_sales.getvalue())
                        success, file_id, modified_time = update_file_by_id(service, FILE_MAPPING["sales"]["id"], file_content)
                        upload_results["Sales"] = success
                        if success:
                            st.success(f"✅ Sales file uploaded successfully")
                        else:
                            st.error("❌ Failed to upload Sales file")
                    
                    # ========== 修复部分：添加 Price File 上传逻辑 ==========
                    # Upload Price file
                    if uploaded_file_price is not None:
                        file_content = io.BytesIO(uploaded_file_price.getvalue())
                        success, file_id, modified_time = update_file_by_id(service, FILE_MAPPING["price"]["id"], file_content)
                        upload_results["Price"] = success
                        if success:
                            st.success(f"✅ Price file uploaded successfully")
                        else:
                            st.error("❌ Failed to upload Price file")
                    # ========== 修复部分结束 ==========
                    
                    # Summary
                    st.divider()
                    successful = sum(1 for v in upload_results.values() if v)
                    st.info(f"📊 Upload Summary: {successful}/{len(upload_results)} files uploaded successfully")
