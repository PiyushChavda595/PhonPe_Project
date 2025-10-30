# Home.py
import io
import pandas as pd
import streamlit as st
import mysql.connector
import os
from streamlit_player import st_player
# style_metric_cards is not needed if style.css is handling it
from streamlit_extras.add_vertical_space import add_vertical_space

# --- Page Config ---
st.set_page_config(
    page_title='PhonePe Pulse | Home',
    layout='wide',
    page_icon='C:/Users/a2z/Desktop/Internship/PhonePe_Project/Logo.png' # Using an emoji
)

# --- Hide Streamlit elements ---
st.markdown("""<style> footer {visibility: hidden;} </style>""", unsafe_allow_html=True) # Hide footer
st.markdown("""<style>.css-1jc7ptx, .e1ewe7hr3, .viewerBadge_container__1QSob, .styles_viewerBadge__1yB5_, .viewerBadge_link__1S137, .viewerBadge_text__1JaDK {display: none;}</style>""", unsafe_allow_html=True)

# --- Load CSS ---
def load_css(file_name):
    try:
        with open(file_name) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except FileNotFoundError:
        st.error(f"CSS file '{file_name}' not found.")
load_css("style.css") # Load custom CSS

# --- NEW DB Credentials (using Streamlit Secrets) ---
DB_HOST = st.secrets["database"]["host"]
DB_PORT = st.secrets["database"]["port"]      # Added port
DB_USER = st.secrets["database"]["user"]
DB_PASSWORD = st.secrets["database"]["password"]
DB_NAME = st.secrets["database"]["db_name"]
DB_SSL_CA = st.secrets["database"]["ssl_ca"]  # Added SSL CA file

# --- DB Fetch Function ---
@st.cache_data(ttl=3600) # Cache data for 1 hour
def fetch_data(query):
    try:
        # This connection now includes port and SSL info
        conn = mysql.connector.connect(
            host=DB_HOST, 
            port=DB_PORT,
            user=DB_USER, 
            password=DB_PASSWORD, 
            database=DB_NAME,
            ssl_ca=DB_SSL_CA,            # This is the new, critical part
            ssl_verify_cert=True         # Force SSL verification
        )
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Safe numeric conversion
        for col in df.select_dtypes(include=['number']).columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        # Convert Pincode to string if exists
        if 'Pincode' in df.columns:
            df['Pincode'] = df['Pincode'].astype(str)
        return df
    
    except mysql.connector.Error as err:
        # Show the error on the Streamlit app itself
        st.error(f"Database Error: {err}") 
        return pd.DataFrame() # Return empty on error

# --- Helper Function for Formatting ---
def format_number_cr(num):
    if num is None or pd.isna(num) or num == 0: return "0 Cr"
    try:
        num = float(num) # Ensure it's a float
        if num >= 10000000:
            return f"{num / 10000000:.2f} Cr"
        return f"{num / 100000:.2f} Lac"
    except (ValueError, TypeError):
        return "N/A" # Handle non-numeric gracefully

# --- === PAGE START === ---

st.title(':violet[PhonePe Data Visualization]')
add_vertical_space(1)

phonepe_description = """
PhonePe has launched PhonePe Pulse, a data analytics platform that provides insights into how Indians are using digital payments.
With over 30 crore registered users and 2000 crore transactions, PhonePe, India's largest digital payments platform with 46% UPI market share,
has a unique ring-side view into the Indian digital payments story. Through this app, you can now easily access and visualize the data provided
by PhonePe Pulse, gaining deep insights and interesting trends into how India transacts with digital payments.
"""
st.write(phonepe_description)
add_vertical_space(1)

# YouTube Video
st_player(url="https://www.youtube.com/watch?v=c_1H6vivsiA", height=480)
add_vertical_space(2)

# --- Metric Cards ---
col1, col2, col3 = st.columns(3)

# Fetch data for metrics with spinner
with st.spinner("Loading key metrics..."):
    total_reg_users_query = "SELECT SUM(RegisteredUsers) as TotalValue FROM top_user"
    total_app_opens_query = "SELECT SUM(AppOpens) as TotalValue FROM map_user"
    total_trans_count_query = "SELECT SUM(Transaction_count) as TotalValue FROM map_transaction"

    df_users = fetch_data(total_reg_users_query)
    df_opens = fetch_data(total_app_opens_query)
    df_count = fetch_data(total_trans_count_query)

    total_reg_users = df_users['TotalValue'].iloc[0] if not df_users.empty and not pd.isna(df_users['TotalValue'].iloc[0]) else 0
    total_app_opens = df_opens['TotalValue'].iloc[0] if not df_opens.empty and not pd.isna(df_opens['TotalValue'].iloc[0]) else 0
    total_trans_count = df_count['TotalValue'].iloc[0] if not df_count.empty and not pd.isna(df_count['TotalValue'].iloc[0]) else 0
    total_trans_count_display = format_number_cr(total_trans_count)

# Display metrics
col1.metric(label='Total Registered Users', value=format_number_cr(total_reg_users), delta='↗️') # Using emoji for trend
col2.metric(label='Total App Opens', value=format_number_cr(total_app_opens), delta='↗️')
col3.metric(label='Total Transaction Count', value=total_trans_count_display, delta='↗️')

# style_metric_cards() # This call is not needed as style.css handles the styling
add_vertical_space(2)

# --- Dataset Exploration Section ---
st.markdown("---")
st.subheader(":violet[Explore Raw Datasets]")
add_vertical_space(1)

dataset_options_display = {
    'Aggregate Transaction': 'aggregated_transaction', 'Aggregate User': 'aggregated_user',
    'Map Transaction': 'map_transaction', 'Map User': 'map_user',
    'Top Transaction': 'top_transaction', 'Top User': 'top_user',
    'Aggregate Insurance': 'aggregated_insurance', 'Map Insurance': 'map_insurance',
    'Top Insurance': 'top_insurance'
}

col_select_home, buff_select_home = st.columns([1, 2])
selected_display_name = col_select_home.selectbox(label='Select Dataset:', options=list(dataset_options_display.keys()), key='home_df_select')
table_name = dataset_options_display[selected_display_name]

# Fetch sample data with spinner
with st.spinner(f"Loading sample for {selected_display_name}..."):
    df_selected = fetch_data(f"SELECT * FROM {table_name} LIMIT 500") # Limit rows for sample

if not df_selected.empty:
    tab1, tab2 = st.tabs(['Show Dataset Sample', 'Download Full Dataset'])
    with tab1:
        st.info(f"Displaying first 100 rows of '{selected_display_name}'.")
        st.dataframe(df_selected.head(100), use_container_width=True)

    with tab2:
        st.subheader(f"Download Full '{selected_display_name}' Data")
        col_dl1_home, col_dl2_home, col_dl3_home = st.columns(3)

        @st.cache_data(ttl=600)
        def get_full_data(tbl_name):
            with st.spinner(f"Fetching full data for {tbl_name}..."): # Add spinner for full download fetch
                 return fetch_data(f"SELECT * FROM {tbl_name}")

        # Add a button to trigger full data fetch and download preparation
        if st.button(f"Prepare Full '{selected_display_name}' for Download", key=f"prep_{table_name}"):
            df_full = get_full_data(table_name)
            if not df_full.empty:
                csv = df_full.to_csv(index=False).encode('utf-8')
                json_data = df_full.to_json(orient='records').encode('utf-8')
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                    df_full.to_excel(writer, index=False, sheet_name='Sheet1')
                excel_bytes = excel_buffer.getvalue()

                # Store prepared data in session state
                st.session_state[f'csv_{table_name}'] = csv
                st.session_state[f'json_{table_name}'] = json_data
                st.session_state[f'excel_{table_name}'] = excel_bytes
                st.success("Download files ready!")
            else:
                st.error("Could not fetch full data.")

        # Display download buttons only if data is prepared
        if f'csv_{table_name}' in st.session_state:
            col_dl1_home.download_button(label="Download CSV", data=st.session_state[f'csv_{table_name}'], file_name=f'{selected_display_name}.csv', mime='text/csv', key=f'home_csv_dl_{table_name}')
        if f'json_{table_name}' in st.session_state:
            col_dl2_home.download_button(label="Download JSON", data=st.session_state[f'json_{table_name}'], file_name=f'{selected_display_name}.json', mime='application/json', key=f'home_json_dl_{table_name}')
        if f'excel_{table_name}' in st.session_state:
            col_dl3_home.download_button(label="Download Excel", data=st.session_state[f'excel_{table_name}'], file_name=f'{selected_display_name}.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', key=f'home_excel_dl_{table_name}')

else:
    st.warning(f"Could not fetch sample data for table: {table_name}")

st.markdown("---")
st.caption("Data Source: PhonePe Pulse | Enhanced Visualization")