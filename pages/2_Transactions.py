# pages/2_Transaction.py
import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px
import json
import os
from streamlit_extras.add_vertical_space import add_vertical_space

# --- Page Config ---
st.set_page_config(page_title='PhonePe Pulse | Transaction', layout='wide', page_icon='C:/Users/a2z/Desktop/Internship/PhonePe_Project/Logo.png')

# --- NEW DB Credentials (using Streamlit Secrets) ---
DB_HOST = st.secrets["database"]["host"]
DB_PORT = st.secrets["database"]["port"]
DB_USER = st.secrets["database"]["user"]
DB_PASSWORD = st.secrets["database"]["password"]
DB_NAME = st.secrets["database"]["db_name"]
DB_SSL_CA = st.secrets["database"]["ssl_ca"]
COORDS_FILE = "district_coords.csv" # Needed for scatter mapbox

# --- DB Fetch Function ---
@st.cache_data(ttl=3600)
def fetch_data(query):
    # Spinner is now outside this function where it's called
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            ssl_ca=DB_SSL_CA,
            ssl_verify_cert=True
        )
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except mysql.connector.Error as err:
        st.error(f"Database Error: {err}")
        return pd.DataFrame()

# --- Load Coords ---
@st.cache_data
def load_coordinates(file_path):
    try:
        df = pd.read_csv(file_path)
        df = df.rename(columns={'Latitude': 'lat', 'Longitude': 'lon',
                                'latitude': 'lat', 'longitude': 'lon',
                                'LATITUDE': 'lat', 'LONGITUDE': 'lon'})
        if 'lat' not in df.columns or 'lon' not in df.columns:
            st.error(f"Coordinate file '{file_path}' must contain Latitude/Longitude columns.")
            return None
        actual_district_column_name = 'District Name' # Use the name from your CSV
        if actual_district_column_name not in df.columns:
            st.error(f"Coordinate file '{file_path}' missing column: '{actual_district_column_name}'.")
            st.warning(f"Available columns: {', '.join(df.columns)}")
            return None
        df['District_Lower'] = df[actual_district_column_name].astype(str).str.lower().str.strip()
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
        df.dropna(subset=['lat', 'lon'], inplace=True)
        return df[['District_Lower', 'lat', 'lon']].drop_duplicates(subset=['District_Lower'])
    except FileNotFoundError:
        st.error(f"Coordinate file '{file_path}' not found. Scatter map requires this.")
        return None
    except Exception as e:
        st.error(f"Error loading coordinate file '{file_path}': {e}")
        return None

# Load coordinates at the start
coords_df = load_coordinates(COORDS_FILE)

# --- Hide elements ---
st.markdown("""<style> footer {visibility: hidden;} </style>""", unsafe_allow_html=True)
st.markdown("""<style>.css-1jc7ptx, .e1ewe7hr3, .viewerBadge_container__1QSob, .styles_viewerBadge__1yB5_, .viewerBadge_link__1S137, .viewerBadge_text__1JaDK {display: none;}</style>""", unsafe_allow_html=True)

st.title(':violet[Transaction Analysis]')
add_vertical_space(2)

# --- Fetch Initial Data for Filters ---
with st.spinner("Loading filter options..."):
    states_df = fetch_data("SELECT DISTINCT State FROM aggregated_transaction ORDER BY State")
    years_df = fetch_data("SELECT DISTINCT Year FROM aggregated_transaction ORDER BY Year DESC")
    quarters_df = fetch_data("SELECT DISTINCT Quarter FROM aggregated_transaction ORDER BY Quarter")

states = states_df['State'].tolist() if not states_df.empty else []
years = years_df['Year'].tolist() if not years_df.empty else []
quarters = quarters_df['Quarter'].tolist() if not quarters_df.empty else []
quarter_options = ["All"] + quarters

# --- 1. Transaction Amount Breakdown by Type (Bar Chart) ---
st.subheader(':blue[Transaction Amount Breakdown by Type]')
col1a, col1b, col1c = st.columns([2, 1, 1])
state1 = col1a.selectbox("State", states, key='state1_trans_type_pg2')
year1 = col1b.selectbox("Year", years, key='year1_trans_type_pg2')
quarter1 = col1c.selectbox("Quarter", quarter_options, key='quarter1_trans_type_pg2')

if state1 and year1:
    with st.spinner(f"Loading transaction type data for {state1} ({year1} Q{quarter1})..."):
        query1 = f"SELECT Transaction_type, SUM(Transaction_amount) as TotalAmount, SUM(Transaction_count) as TotalCount, Quarter FROM aggregated_transaction WHERE State = '{state1}' AND Year = {year1}"
        if quarter1 != 'All':
            query1 += f" AND Quarter = {int(quarter1)}"
        query1 += " GROUP BY Transaction_type, Quarter ORDER BY TotalAmount DESC"
        df1 = fetch_data(query1)

    if not df1.empty:
        if quarter1 == 'All':
            df1_agg = df1.groupby('Transaction_type').agg(TotalAmount=('TotalAmount', 'sum'), TotalCount=('TotalCount', 'sum')).reset_index()
            df1_agg['Quarter'] = 'All'
        else:
            df1_agg = df1

        fig1 = px.bar(
            df1_agg, x="Transaction_type", y="TotalAmount",
            color="Transaction_type",
            title=f"Transaction Amounts in {state1} ({year1}{f', Q{quarter1}' if quarter1 != 'All' else ''})",
            labels={'TotalAmount': 'Total Transaction Amount (₹)', 'Transaction_type': 'Transaction Type'},
            hover_data={'TotalCount':':,', 'Quarter':True} # Format TotalCount in hover
        )
        fig1.update_traces(hovertemplate="<b>Type:</b> %{x}<br><b>Amount:</b> ₹%{y:,.0f}<br><b>Count:</b> %{customdata[0]:,}<br><b>Quarter:</b> %{customdata[1]}<extra></extra>")
        fig1.update_layout(showlegend=False, title_x=0.5, width=900, height=500)
        fig1.update_traces(marker_line=dict(width=1, color='DarkSlateGrey'))
        st.plotly_chart(fig1, use_container_width=True) # Use container width
        with st.expander('View Data'):
            st.dataframe(df1[['Quarter', 'Transaction_type', 'TotalAmount', 'TotalCount']].reset_index(drop=True))
    else:
        st.warning("No data found for the selected filters.")
else:
    st.info("Please select a State and Year.")
add_vertical_space(2)

# --- 2. Transaction Hotspots (Scatter Mapbox) ---
st.subheader(':blue[Transaction Hotspots - Districts]')
col2a, col2b, buff2 = st.columns([1, 1, 4])
year2 = col2a.selectbox("Year", years, key='year2_hotspot_pg2')
quarter2 = col2b.selectbox("Quarter", quarter_options, key='quarter2_hotspot_pg2')

if coords_df is not None and year2:
    with st.spinner(f"Loading hotspot data for {year2} Q{quarter2}..."):
        query2 = f"SELECT State, District, SUM(Transaction_amount) as TotalAmount, SUM(Transaction_count) as TotalCount, Quarter FROM map_transaction WHERE Year = {year2}"
        if quarter2 != 'All':
            query2 += f" AND Quarter = {int(quarter2)}"
        query2 += " GROUP BY State, District, Quarter HAVING SUM(Transaction_amount) > 0"
        df2_trans = fetch_data(query2)

    if not df2_trans.empty:
        df2_trans['District_Lower'] = df2_trans['District'].astype(str).str.lower().str.strip()
        df2_merged = pd.merge(df2_trans, coords_df, on='District_Lower', how='left')
        df2_merged.dropna(subset=['lat', 'lon'], inplace=True)

        if not df2_merged.empty:
            if quarter2 == 'All':
                df2_plot = df2_merged.groupby(['State', 'District', 'District_Lower', 'lat', 'lon']).agg(
                    TotalAmount=('TotalAmount', 'sum'),
                    TotalCount=('TotalCount', 'sum')
                ).reset_index()
                df2_plot['Quarter'] = 'All'
            else:
                df2_plot = df2_merged

            fig2 = px.scatter_mapbox(df2_plot, lat="lat", lon="lon",
                                    size="TotalAmount", hover_name="District",
                                    # --- THIS IS THE FIX ---
                                    hover_data={"State": True,
                                                "TotalCount": ':,',
                                                "TotalAmount": ':,.0f',
                                                'Quarter': True,
                                                'lat': False, # Hide lat/lon from hover
                                                'lon': False,
                                                'District_Lower': False
                                                # Removed 'size': False
                                                },
                                    # --- END OF FIX ---
                                    title=f"Transaction Hotspots ({year2}{f', Q{quarter2}' if quarter2 != 'All' else ''})",
                                    size_max=40, zoom=3.8, center={"lat": 20.5937, "lon": 78.9629},
                                    color="TotalAmount", # Color points by amount too
                                    color_continuous_scale=px.colors.sequential.Plasma_r,
                                    labels={'TotalAmount':'Total Amount (₹)', 'TotalCount':'Total Count'}
                                    )
            fig2.update_layout(mapbox_style='carto-positron', margin={"r":0,"t":40,"l":0,"b":0}, width=900, height=500)
            st.plotly_chart(fig2, use_container_width=True) # Use container width
            with st.expander('View Mapped Data'):
                st.dataframe(df2_merged[['State', 'District', 'Quarter', 'TotalAmount', 'TotalCount', 'lat', 'lon']].reset_index(drop=True))
        else:
            st.warning("No districts could be mapped. Check names in DB vs coordinate file.")
    else:
        st.warning("No transaction data found for the selected filters.")
elif not year2:
    st.info("Please select a Year for Hotspot analysis.")
else:
    # Error message already shown by load_coordinates
    pass
add_vertical_space(2)

# --- 3. Transaction Count Proportion (Pie Chart) ---
st.subheader(":blue[Transaction Count Proportion by Type]")
col3a, col3b, col3c = st.columns([2, 1, 1])
state3 = col3a.selectbox('State', options=states, key='state3_pie_pg2')
year3 = col3b.selectbox('Year', options=years, key='year3_pie_pg2')
quarter3 = col3c.selectbox('Quarter', options=quarter_options, key='quarter3_pie_pg2')

if state3 and year3:
    with st.spinner(f"Loading count data for {state3} ({year3} Q{quarter3})..."):
        query3 = f"SELECT Transaction_type, SUM(Transaction_count) as TotalCount, Quarter FROM aggregated_transaction WHERE State = '{state3}' AND Year = {year3}"
        if quarter3 != 'All':
            query3 += f" AND Quarter = {int(quarter3)}"
        query3 += " GROUP BY Transaction_type, Quarter HAVING SUM(Transaction_count) > 0 ORDER BY TotalCount DESC" # Filter zero counts
        df3 = fetch_data(query3)

    if not df3.empty:
        if quarter3 == 'All':
            df3_agg = df3.groupby('Transaction_type')['TotalCount'].sum().reset_index()
            df3_agg['Quarter'] = 'All'
        else:
            df3_agg = df3

        fig3 = px.pie(
            df3_agg, names='Transaction_type', values='TotalCount', hole=.4,
            title=f"Transaction Count Share in {state3} ({year3}{f', Q{quarter3}' if quarter3 != 'All' else ''})",
            hover_data=['Quarter']
        )
        fig3.update_traces(hovertemplate="<b>Type:</b> %{label}<br><b>Count:</b> %{value:,}<br><b>Share:</b> %{percent}<extra></extra>")
        fig3.update_layout(width=900, height=500, title_x=0.5)
        st.plotly_chart(fig3, use_container_width=True) # Use container width
        with st.expander('View Data'):
            st.dataframe(df3[['Quarter', 'Transaction_type', 'TotalCount']].reset_index(drop=True))
    else:
        st.warning("No data found for the selected filters.")
else:
    st.info("Please select a State and Year.")