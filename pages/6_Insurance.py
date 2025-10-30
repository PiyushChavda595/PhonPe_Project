# pages/6_Insurance.py
import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px
import json
import os
from streamlit_extras.add_vertical_space import add_vertical_space

# --- Page Config ---
st.set_page_config(page_title='PhonePe Pulse | Insurance', layout='wide', page_icon='Logo.png')

# --- NEW DB Credentials (using Streamlit Secrets) ---
DB_HOST = st.secrets["database"]["host"]
DB_PORT = st.secrets["database"]["port"]
DB_USER = st.secrets["database"]["user"]
DB_PASSWORD = st.secrets["database"]["password"]
DB_NAME = st.secrets["database"]["db_name"]
DB_SSL_CA = st.secrets["database"]["ssl_ca"]
GEOJSON_FILE = "india_states.geojson"
COORDS_FILE = "district_coords.csv"

# --- DB Fetch Function ---
@st.cache_data(ttl=3600)
def fetch_data(query):
    with st.spinner("Fetching insurance data..."): # Add spinner
        try:
            # This is the updated connection call for Aiven
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
            
            # Ensure Count/Amount are numeric (from your original file)
            if 'Count' in df.columns: 
                df['Count'] = pd.to_numeric(df['Count'], errors='coerce').fillna(0)
            if 'Amount' in df.columns: 
                df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce').fillna(0)
            
            return df
        except mysql.connector.Error as err:
            st.error(f"Database Error: {err}")
            return pd.DataFrame()

# --- Load Coords & GeoJSON ---
@st.cache_data
def load_coordinates(file_path):
    # ... (Same function as in other pages, using 'District Name') ...
    try:
        df = pd.read_csv(file_path)
        df = df.rename(columns={'Latitude': 'lat', 'Longitude': 'lon'})
        if 'lat' not in df.columns or 'lon' not in df.columns: 
            return None
        actual_district_column_name = 'District Name' # Use correct name
        if actual_district_column_name not in df.columns: 
            return None
        df['District_Lower'] = df[actual_district_column_name].astype(str).str.lower().str.strip()
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
        df.dropna(subset=['lat', 'lon'], inplace=True)
        return df[['District_Lower', 'lat', 'lon']].drop_duplicates(subset=['District_Lower'])
    except: 
        return None # Simplified error handling

@st.cache_data
def load_geojson(file_path):
    try:
        with open(file_path, 'r') as f: return json.load(f)
    except: 
        return None
coords_df = load_coordinates(COORDS_FILE)
geojson_data = load_geojson(GEOJSON_FILE)

# --- State Name Correction ---
state_name_mapping = {
    'Andaman & Nicobar Islands': 'Andaman & Nicobar',
    'Dadra & Nagar Haveli & Daman & Diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'Delhi': 'National Capital Territory of Delhi',
    'Jammu & Kashmir': 'Jammu & Kashmir', 'Ladakh': 'Ladakh', 'Telengana': 'Telangana'
}

# --- Hide elements ---
st.markdown("""<style> footer {visibility: hidden;} </style>""", unsafe_allow_html=True)
st.markdown("""<style>.css-1jc7ptx, .e1ewe7hr3, .viewerBadge_container__1QSob, .styles_viewerBadge__1yB5_, .viewerBadge_link__1S137, .viewerBadge_text__1JaDK {display: none;}</style>""", unsafe_allow_html=True)

st.title(':violet[Insurance Analysis]')
add_vertical_space(2)

# --- Fetch Initial Data for Filters ---
try:
    states_df = fetch_data("SELECT DISTINCT State FROM aggregated_insurance ORDER BY State")
    years_df = fetch_data("SELECT DISTINCT Year FROM aggregated_insurance ORDER BY Year DESC")
    quarters_df = fetch_data("SELECT DISTINCT Quarter FROM aggregated_insurance ORDER BY Quarter")
    states = states_df['State'].tolist() if not states_df.empty else []
    years = years_df['Year'].tolist() if not years_df.empty else []
    quarters = quarters_df['Quarter'].tolist() if not quarters_df.empty else []
    quarter_options = ["All"] + quarters
except Exception as e:
    st.error(f"Error fetching filter options: {e}")
    states, years, quarters = [], [], []
    quarter_options = ['All']

# --- Visualizations ---
tab1, tab2, tab3 = st.tabs(["State Totals", "District Map", "Top Pincodes"])

with tab1:
    st.subheader("Insurance Count & Amount by State")
    col1a, col1b = st.columns(2)
    year1 = col1a.selectbox("Year", years, key="ins_state_year")
    quarter1 = col1b.selectbox("Quarter", quarter_options, key="ins_state_qtr")

    if year1:
        query1 = f"SELECT State, SUM(Count) as TotalCount, SUM(Amount) as TotalAmount FROM aggregated_insurance WHERE Year={year1}"
        if quarter1 != 'All': 
            query1 += f" AND Quarter={int(quarter1)}"
        query1 += " GROUP BY State ORDER BY State"
        df1 = fetch_data(query1)

        if not df1.empty:
            col1_chart, col2_chart = st.columns(2)
            with col1_chart:
                fig1_count = px.bar(df1.sort_values("TotalCount", ascending=False).head(15), x="TotalCount", y="State", orientation='h', title="Top 15 States by Insurance Count")
                fig1_count.update_layout(yaxis={'categoryorder':'total ascending'}, title_x=0.5)
                st.plotly_chart(fig1_count, use_container_width=True)
            with col2_chart:
                fig1_amount = px.bar(df1.sort_values("TotalAmount", ascending=False).head(15), x="TotalAmount", y="State", orientation='h', title="Top 15 States by Insurance Amount (₹)")
                fig1_amount.update_layout(yaxis={'categoryorder':'total ascending'}, title_x=0.5)
                st.plotly_chart(fig1_amount, use_container_width=True)
            with st.expander("View State Data"): 
                st.dataframe(df1)
        else: 
            st.warning("No aggregated insurance data found.")
    else: 
        st.info("Select Year.")

with tab2:
    st.subheader("District-wise Insurance Map")
    col2a, col2b = st.columns(2)
    year2 = col2a.selectbox("Year", years, key="ins_map_year")
    quarter2 = col2b.selectbox("Quarter", quarter_options, key="ins_map_qtr")
    metric2 = st.radio("Select Metric:", ("Count", "Amount"), key="ins_map_metric", horizontal=True)

    if year2 and coords_df is not None:
        query2 = f"SELECT State, District, SUM(Count) as TotalCount, SUM(Amount) as TotalAmount, Quarter FROM map_insurance WHERE Year={year2}"
        if quarter2 != 'All': 
            query2 += f" AND Quarter={int(quarter2)}"
        query2 += " GROUP BY State, District, Quarter"
        df2_map = fetch_data(query2)

        if not df2_map.empty:
            df2_map['District_Lower'] = df2_map['District'].astype(str).str.lower().str.strip()
            df2_merged = pd.merge(df2_map, coords_df, on='District_Lower', how='left')
            df2_merged.dropna(subset=['lat', 'lon'], inplace=True)

            if not df2_merged.empty:
                if quarter2 == 'All':
                    df2_plot = df2_merged.groupby(['State', 'District', 'District_Lower', 'lat', 'lon']).agg(TotalCount=('TotalCount','sum'), TotalAmount=('TotalAmount','sum')).reset_index()
                    df2_plot['Quarter'] = 'All'
                else: 
                    df2_plot = df2_merged

                size_col = "TotalCount" if metric2 == "Count" else "TotalAmount"
                fig2_map = px.scatter_mapbox(df2_plot, lat="lat", lon="lon", size=size_col,
                                            hover_name="District", hover_data=["State", "TotalCount", "TotalAmount", 'Quarter'],
                                            title=f"Insurance {metric2} Hotspots ({year2}{f', Q{quarter2}' if quarter2 != 'All' else ''})",
                                            size_max=30, zoom=3.8, center={"lat": 20.5937, "lon": 78.9629},
                                            color_continuous_scale=px.colors.sequential.Viridis ) # Use size for primary metric

                fig2_map.update_layout(mapbox_style='carto-positron', margin={"r":0,"t":40,"l":0,"b":0})
                st.plotly_chart(fig2_map, use_container_width=True)
                with st.expander("View Mapped Data"): 
                    st.dataframe(df2_merged)
            else: 
                st.warning("No districts could be mapped.")
        else: 
            st.warning("No map insurance data found.")
    elif not year2: 
        st.info("Select Year.")
    else: 
        st.warning("Coordinate file missing.")


with tab3:
    st.subheader("Top Pincodes for Insurance")
    col3a, col3b = st.columns(2)
    year3 = col3a.selectbox("Year", years, key="ins_pin_year")
    quarter3 = col3b.selectbox("Quarter", quarter_options, key="ins_pin_qtr")
    metric3 = st.radio("Select Metric:", ("Count", "Amount"), key="ins_pin_metric", horizontal=True)

    if year3:
        sort_col = "TotalCount" if metric3 == "Count" else "TotalAmount"
        query3 = f"SELECT State, Pincode, SUM(Count) as TotalCount, SUM(Amount) as TotalAmount FROM top_insurance WHERE Year={year3}"
        if quarter3 != 'All': 
            query3 += f" AND Quarter={int(quarter3)}"
        query3 += f" GROUP BY State, Pincode ORDER BY {sort_col} DESC LIMIT 10"
        df3_pin = fetch_data(query3)

        if not df3_pin.empty:
            df3_pin['Pincode'] = df3_pin['Pincode'].astype(str) # Ensure pincode is string for axis
            fig3_pin = px.bar(df3_pin, x=sort_col, y="Pincode", orientation='h',
                            title=f"Top 10 Pincodes by Insurance {metric3} ({year3}{f', Q{quarter3}' if quarter3 != 'All' else ''})",
                            hover_data=["State"])
            fig3_pin.update_layout(yaxis={'categoryorder':'total ascending'}, title_x=0.5)
            st.plotly_chart(fig3_pin, use_container_width=True)
            with st.expander("View Top Pincode Data"): 
                st.dataframe(df3_pin)
        else: 
            st.warning("No top pincode insurance data found.")
    else: 
        st.info("Select Year.")
