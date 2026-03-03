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
tab1, tab2 = st.tabs(["BigSeller", "PO"])
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
    uploaded_file_po = st.file_uploader("Please upload the PO Excel file", type=["xlsx", "xls"], key="po_uploader")
    if uploaded_file_po is not None:
        if st.button("🚀 Process PO File", key="po_process"):
            with st.spinner('Processing in progress, please wait...'):
                result_df, error_file_data = process_po_file(uploaded_file_po)
                if result_df is not None:
                    # Extract error file data
                    error_rows = error_file_data.get('error_rows', [])
                    scan_needed = error_file_data.get('scan_needed', [])
                    adjustment_needed = error_file_data.get('adjustment_needed', [])
                    manual_input_dict = error_file_data.get('manual_input_needed', {})
                    
                    if manual_input_dict:
                        st.warning(f"Warning: {len(manual_input_dict)} goods require manual Stock Code input.")
                        with st.expander(f"📝 Enter Stock Codes ({len(manual_input_dict)} items)", expanded=True):
                            manual_stock_codes = {}
                            for idx, goods_name in enumerate(manual_input_dict.keys(), 1):
                                col1, col2 = st.columns([3, 1])
                                with col1:
                                    st.text(f"{idx}. {goods_name}")
                                with col2:
                                    manual_stock_codes[goods_name] = st.text_input("", key=f"manual_{goods_name}", placeholder="Enter code")
                            if st.button("✅ Confirm Manual Inputs", key="confirm_manual"):
                                confirmed_count = 0
                                for goods_name, stock_code in manual_stock_codes.items():
                                    if stock_code.strip():
                                        add_to_stock_imei_mapping(goods_name, stock_code)
                                        result_df.loc[result_df['Goods name'] == goods_name, 'Stock Code'] = stock_code
                                        result_df.loc[result_df['Goods name'] == goods_name, 'Manual Input Required'] = False
                                        confirmed_count += 1
                                st.success(f"✓ {confirmed_count} items confirmed and saved to database.")
                    st.divider()
                    st.subheader("Download Results")
                    # Complete: has Stock Code AND IMEI is not ERROR
                    complete_df = result_df[
                        (result_df['Stock Code'].notna()) & 
                        (~result_df['IMEI Status'].str.contains('ERROR', na=False))
                    ].copy()
                    # Incomplete Stock Code: missing Stock Code
                    incomplete_stock_df = result_df[
                        result_df['Stock Code'].isna()
                    ].copy()
                    # Incomplete IMEI: has Stock Code but IMEI is ERROR
                    incomplete_imei_df = result_df[
                        (result_df['Stock Code'].notna()) & 
                        (result_df['IMEI Status'].str.contains('ERROR', na=False))
                    ].copy()
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Complete Records", len(complete_df))
                        if len(complete_df) > 0:
                            output_complete = complete_df[['Stock Code', 'IMEI', 'DO Number', 'Medium Box Code']].copy()
                            output_buffer = io.BytesIO()
                            with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                                output_complete.to_excel(writer, index=False, sheet_name='Complete')
                            st.download_button(label="📥 Download Complete", data=output_buffer.getvalue(), file_name="output_po_complete.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="po_complete_download")
                    with col2:
                        st.metric("Incomplete Stock Code", len(incomplete_stock_df))
                        if len(incomplete_stock_df) > 0:
                            output_incomplete_stock = incomplete_stock_df[['Goods name', 'DO Number', 'Medium Box Code', 'SN', 'Processing Status']].copy()
                            output_buffer = io.BytesIO()
                            with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                                output_incomplete_stock.to_excel(writer, index=False, sheet_name='Missing Stock Code')
                            st.download_button(label="📥 Download Stock Code", data=output_buffer.getvalue(), file_name="output_po_incomplete_stock.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="po_incomplete_stock_download")
                    with col3:
                        st.metric("Incomplete IMEI", len(incomplete_imei_df))
                        if len(incomplete_imei_df) > 0:
                            output_incomplete_imei = incomplete_imei_df[['Stock Code', 'Goods name', 'IMEI', 'DO Number', 'IMEI Status']].copy()
                            output_buffer = io.BytesIO()
                            with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                                output_incomplete_imei.to_excel(writer, index=False, sheet_name='IMEI Issues')
                            st.download_button(label="📥 Download IMEI Issues", data=output_buffer.getvalue(), file_name="output_po_incomplete_imei.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="po_incomplete_imei_download")
                    with col4:
                        error_count = len(error_rows) + len(scan_needed) + len(adjustment_needed)
                        st.metric("Error/Action Items", error_count)
                        if error_count > 0:
                            # Create error file
                            error_sheet_data = []
                            
                            # Add error rows
                            if error_rows:
                                error_df = result_df.iloc[error_rows].copy()
                                error_sheet_data.append(("Error Rows", error_df))
                            
                            # Add scan needed list
                            if scan_needed:
                                scan_df = pd.DataFrame({
                                    'Goods Name': scan_needed,
                                    'Action': ['Needs manual IMEI scan'] * len(scan_needed)
                                })
                                error_sheet_data.append(("Scan Needed", scan_df))
                            
                            # Add adjustment needed list
                            if adjustment_needed:
                                adjust_df = pd.DataFrame({
                                    'Goods Name': adjustment_needed,
                                    'Action': ['Update batch_control to true'] * len(adjustment_needed)
                                })
                                error_sheet_data.append(("Adjustment Needed", adjust_df))
                            
                            # Add manual input needed list
                            if manual_input_dict:
                                manual_df = pd.DataFrame({
                                    'Goods Name': list(manual_input_dict.keys()),
                                    'Action': ['Manual Stock Code input needed'] * len(manual_input_dict)
                                })
                                error_sheet_data.append(("Manual Input Needed", manual_df))
                            
                            # Create Excel file with multiple sheets
                            output_error = io.BytesIO()
                            with pd.ExcelWriter(output_error, engine='openpyxl') as writer:
                                for sheet_name, df in error_sheet_data:
                                    df.to_excel(writer, index=False, sheet_name=sheet_name[:31])  # Excel sheet name limit is 31 chars
                            
                            st.download_button(label="📥 Download Error/Action File", data=output_error.getvalue(), file_name="output_po_errors.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="po_error_download")
                    
                    # --- Statistics Section ---
                    st.divider()
                    st.subheader("📊 Processing Statistics")
                    
                    # Left-Right Layout with Spacing
                    left_col, spacer_col, right_col = st.columns([2, 0.5, 2])
                    
                    # LEFT COLUMN: Stock Code Distribution
                    with left_col:
                        st.markdown("#### Stock Code Distribution")
                        stock_code_complete = len(result_df[result_df['Stock Code'].notna()])
                        stock_code_incomplete = len(result_df[result_df['Stock Code'].isna()])
                        
                        col_stats1, col_stats2 = st.columns(2)
                        with col_stats1:
                            st.metric("✅ With Stock Code", stock_code_complete)
                        with col_stats2:
                            st.metric("❌ Missing Stock Code", stock_code_incomplete)
                        
                        # Stock Code Pie Chart
                        if stock_code_complete > 0 or stock_code_incomplete > 0:
                            import plotly.graph_objects as go
                            
                            stock_code_data = [stock_code_complete, stock_code_incomplete]
                            stock_code_labels = [f"With Stock Code ({stock_code_complete})", f"Missing Stock Code ({stock_code_incomplete})"]
                            stock_code_colors = ['#A2CB8B', '#FF7070']
                            
                            fig_stock = go.Figure(data=[go.Pie(
                                labels=stock_code_labels,
                                values=stock_code_data,
                                marker=dict(colors=stock_code_colors),
                                hoverinfo='label+value+percent'
                            )])
                            fig_stock.update_layout(
                                title="Stock Code Completion Status",
                                height=400,
                                showlegend=True
                            )
                            st.plotly_chart(fig_stock, use_container_width=True)
                    
                    # RIGHT COLUMN: IMEI Statistics by Case
                    with right_col:
                        st.markdown("#### IMEI Processing by Case")
                        
                        # Count each case
                        case1_count = len(result_df[result_df['IMEI Status'] == 'Case 1: Used Excel IMEI'])
                        case2_count = len(result_df[result_df['IMEI Status'] == 'Case 2: Needs manual scan'])
                        case3_count = len(result_df[result_df['IMEI Status'] == 'Case 3: Used Excel IMEI (needs adjustment)'])
                        case4_count = len(result_df[result_df['IMEI Status'] == 'Case 4: Skipped (no IMEI needed)'])
                        case5_count = len(result_df[result_df['IMEI Status'].str.contains('ERROR', na=False)])
                        
                        # All 5 cases in one row
                        col_case1, col_case2, col_case3, col_case4, col_case5 = st.columns(5)
                        with col_case1:
                            st.metric("Case 1", case1_count)
                        with col_case2:
                            st.metric("Case 2", case2_count)
                        with col_case3:
                            st.metric("Case 3", case3_count)
                        with col_case4:
                            st.metric("Case 4", case4_count)
                        with col_case5:
                            st.metric("Case 5", case5_count)
                        
                        # IMEI Pie Chart
                        if case1_count > 0 or case2_count > 0 or case3_count > 0 or case4_count > 0 or case5_count > 0:
                            imei_data = [case1_count, case2_count, case3_count, case4_count, case5_count]
                            imei_labels = [
                                f"Case 1: Used ({case1_count})",
                                f"Case 2: Scan ({case2_count})",
                                f"Case 3: Adjust ({case3_count})",
                                f"Case 4: Skip ({case4_count})",
                                f"Case 5: Error ({case5_count})"
                            ]
                            imei_colors = ['#7AAACE', '#FFCE99', '#C9BEFF', '#ACBAC4', '#FF7070']
                            
                            fig_imei = go.Figure(data=[go.Pie(
                                labels=imei_labels,
                                values=imei_data,
                                marker=dict(colors=imei_colors),
                                hoverinfo='label+value+percent'
                            )])
                            fig_imei.update_layout(
                                title="IMEI Distribution",
                                height=400,
                                showlegend=True
                            )
                            st.plotly_chart(fig_imei, use_container_width=True)
