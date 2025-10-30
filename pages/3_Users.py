# pages/3_Users.py
import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px
import json
import os
from streamlit_extras.add_vertical_space import add_vertical_space

# --- Page Config ---
st.set_page_config(page_title='PhonePe Pulse | Users', layout='wide', page_icon='C:/Users/a2z/Desktop/Internship/PhonePe_Project/Logo.png')

# --- NEW DB Credentials (using Streamlit Secrets) ---
DB_HOST = st.secrets["database"]["host"]
DB_PORT = st.secrets["database"]["port"]
DB_USER = st.secrets["database"]["user"]
DB_PASSWORD = st.secrets["database"]["password"]
DB_NAME = st.secrets["database"]["db_name"]
DB_SSL_CA = st.secrets["database"]["ssl_ca"]
GEOJSON_FILE = "india_states.geojson" # Needed for density map outline
COORDS_FILE = "district_coords.csv" # Needed for scatter/density mapbox

# --- DB Fetch Function ---
@st.cache_data(ttl=3600)
def fetch_data(query):
    # Spinner is outside this function
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
        
        # Safe numeric conversion
        for col in df.select_dtypes(include=['number']).columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        # Convert Pincode to string if exists
        if 'Pincode' in df.columns:
            df['Pincode'] = df['Pincode'].astype(str)
            
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
        st.error(f"Coordinate file '{file_path}' not found. Maps require this.")
        return None
    except Exception as e:
        st.error(f"Error loading coordinate file '{file_path}': {e}")
        return None

# --- Load GeoJSON ---
@st.cache_data
def load_geojson(file_path):
    try:
        with open(file_path, 'r') as f: return json.load(f)
    except FileNotFoundError:
        st.warning(f"GeoJSON file '{file_path}' not found. Density map outline will be missing.")
        return None
    except Exception as e:
        st.error(f"Error loading GeoJSON file '{file_path}': {e}")
        return None

# Load files at start
coords_df = load_coordinates(COORDS_FILE)
geojson_data = load_geojson(GEOJSON_FILE)

# --- Hide elements ---
st.markdown("""<style> footer {visibility: hidden;} </style>""", unsafe_allow_html=True)
st.markdown("""<style>.css-1jc7ptx, .e1ewe7hr3, .viewerBadge_container__1QSob, .styles_viewerBadge__1yB5_, .viewerBadge_link__1S137, .viewerBadge_text__1JaDK {display: none;}</style>""", unsafe_allow_html=True)

st.title(':violet[User Analysis]')
add_vertical_space(2)

# --- Fetch Initial Data for Filters ---
with st.spinner("Loading filter options..."):
    try:
        states_df = fetch_data("SELECT DISTINCT State FROM aggregated_user ORDER BY State")
        years_df = fetch_data("SELECT DISTINCT Year FROM aggregated_user ORDER BY Year DESC")
        quarters_df = fetch_data("SELECT DISTINCT Quarter FROM aggregated_user ORDER BY Quarter")
        states = states_df['State'].tolist() if not states_df.empty else []
        state_options = ['All'] + states
        years = years_df['Year'].tolist() if not years_df.empty else []
        quarters = quarters_df['Quarter'].tolist() if not quarters_df.empty else []
        quarter_options = ["All"] + quarters
    except Exception as e:
        st.error(f"Error fetching filter options: {e}")
        states, years, quarters = [], [], []
        state_options, quarter_options = ['All'], ['All']


# --- 1. Transaction Count and Percentage by Brand (Treemap) ---
st.subheader(':blue[Transaction Count and Percentage by Brand]')
col1a, col1b, col1c = st.columns([2, 1, 1])
state1 = col1a.selectbox('State', options=state_options, key='state1_brand_pg3')
year1 = col1b.selectbox('Year', options=years, key='year1_brand_pg3')
quarter1 = col1c.selectbox("Quarter", options=quarter_options, key='quarter1_brand_pg3')

if year1:
    with st.spinner(f"Loading brand data for {state1} ({year1} Q{quarter1})..."):
        query1 = f"SELECT Brand, SUM(Transaction_count) as TotalCount, AVG(Percentage) as AvgPercentage, Quarter FROM aggregated_user WHERE Year = {year1}"
        if state1 != 'All':
            query1 += f" AND State = '{state1}'"
        if quarter1 != 'All':
            query1 += f" AND Quarter = {int(quarter1)}"
        query1 += " GROUP BY Brand, Quarter HAVING SUM(Transaction_count) > 0 ORDER BY TotalCount DESC"
        df1 = fetch_data(query1)

    if not df1.empty:
        if quarter1 == 'All':
            df1_agg = df1.groupby('Brand').agg(TotalCount=('TotalCount', 'sum'), AvgPercentage=('AvgPercentage', 'mean')).reset_index()
            df1_agg['Quarter'] = 'All'
        else:
            df1_agg = df1

        fig1 = px.treemap(
            df1_agg, path=['Brand'], values='TotalCount', color='AvgPercentage',
            color_continuous_scale='YlOrBr',
            hover_data={'AvgPercentage': ':.2%', 'Quarter': True},
            hover_name='Brand',
            title=f"Brand Share in {state1} ({year1}{f', Q{quarter1}' if quarter1 != 'All' else ''})"
        )
        fig1.update_traces(hovertemplate='<b>%{label}</b><br>Transaction Count: %{value:,}<br>Avg. Share: %{color:.2%}<extra></extra>')
        fig1.update_layout(width=900, height=500, title_x=0.5, coloraxis_colorbar=dict(tickformat='.1%', title='Avg % Share'))
        st.plotly_chart(fig1, use_container_width=True)
        with st.expander('View Data'):
            st.dataframe(df1[['Quarter', 'Brand', 'TotalCount', 'AvgPercentage']].reset_index(drop=True))
    else:
        st.warning("No data found for the selected filters (Brands).")
else:
    st.info("Please select a Year for Brand analysis.")
add_vertical_space(2)


# --- 2. Registered Users Hotspots (Scatter Mapbox) ---
st.subheader(':blue[Registered Users Hotspots - District]')
col2a, col2b, col2c = st.columns([2, 1, 1])
state2 = col2a.selectbox('State', options=state_options, key='state2_reg_user_pg3')
year2 = col2b.selectbox('Year', options=years, key='year2_reg_user_pg3')
quarter2 = col2c.selectbox("Quarter", options=quarter_options, key='quarter2_reg_user_pg3')

if coords_df is not None and year2:
    with st.spinner(f"Loading user hotspot data for {state2} ({year2} Q{quarter2})..."):
        query2 = f"SELECT State, District, SUM(RegisteredUsers) as TotalRegisteredUsers, Quarter FROM map_user WHERE Year = {year2}"
        if state2 != 'All':
            query2 += f" AND State = '{state2}'"
        if quarter2 != 'All':
            query2 += f" AND Quarter = {int(quarter2)}"
        query2 += " GROUP BY State, District, Quarter HAVING SUM(RegisteredUsers) > 0"
        df2_user = fetch_data(query2)

    if not df2_user.empty:
        df2_user['District_Lower'] = df2_user['District'].astype(str).str.lower().str.strip()
        df2_merged = pd.merge(df2_user, coords_df, on='District_Lower', how='left')
        df2_merged.dropna(subset=['lat', 'lon'], inplace=True)

        if not df2_merged.empty:
            if quarter2 == 'All':
                df2_plot = df2_merged.groupby(['State', 'District', 'District_Lower', 'lat', 'lon']).agg(
                    TotalRegisteredUsers=('TotalRegisteredUsers', 'sum')
                ).reset_index()
                df2_plot['Quarter'] = 'All'
            else:
                df2_plot = df2_merged

            fig2 = px.scatter_mapbox(
                df2_plot, lat="lat", lon="lon", size="TotalRegisteredUsers",
                hover_name="District",
                hover_data={"State": True,
                            "Quarter": True,
                            "TotalRegisteredUsers": ':,',
                            'lat': False, 'lon': False, 'District_Lower': False},
                title=f"Registered Users in {state2} ({year2}{f', Q{quarter2}' if quarter2 != 'All' else ''})",
                size_max=40, zoom=3.8 if state2 == 'All' else 5, center={"lat": 20.5937, "lon": 78.9629},
                color="TotalRegisteredUsers",
                color_continuous_scale=px.colors.sequential.Agsunset_r,
                labels={'TotalRegisteredUsers': 'Registered Users'}
            )
            fig2.update_layout(mapbox_style='carto-positron', margin={"r":0,"t":40,"l":0,"b":0}, width=900, height=500)
            st.plotly_chart(fig2, use_container_width=True)
            with st.expander('View Mapped Data'):
                st.dataframe(df2_merged[['State', 'District', 'Quarter', 'TotalRegisteredUsers', 'lat', 'lon']].reset_index(drop=True))
        else:
            st.warning("No districts could be mapped. Check names in DB vs coordinate file.")
    else:
        st.warning("No user data found for selected filters.")
elif not year2:
    st.info("Please select a Year for User Hotspot analysis.")
else:
    # Error message already shown by load_coordinates
    pass
add_vertical_space(2)


# --- 3. Top Districts by Registered Users (Bar Chart) ---
st.subheader(':blue[Top Districts by Registered Users]')
col3a, col3b, buff3 = st.columns([2, 1, 3])
state3 = col3a.selectbox('State', options=state_options, key='state3_top_dist_pg3')
year3 = col3b.selectbox('Year', options=years, key='year3_top_dist_pg3')

if year3:
    with st.spinner(f"Loading top districts for {state3} ({year3})..."):
        query3 = f"SELECT State, District, SUM(RegisteredUsers) as TotalRegisteredUsers FROM map_user WHERE Year = {year3}" # Use map_user
        if state3 != 'All':
            query3 += f" AND State = '{state3}'"
        query3 += " GROUP BY State, District ORDER BY TotalRegisteredUsers DESC LIMIT 10"
        df3 = fetch_data(query3)

    if not df3.empty:
        fig3 = px.bar(
            df3, x='TotalRegisteredUsers', y='District', orientation='h',
            color='TotalRegisteredUsers', color_continuous_scale='Greens_r',
            title=f"Top 10 Districts in {state3} ({year3}) by Registered Users",
            labels={'TotalRegisteredUsers':'Total Registered Users'},
            hover_data={'State': True, 'TotalRegisteredUsers': ':,'}
        )
        fig3.update_traces(hovertemplate="<b>District:</b> %{y}<br><b>State:</b> %{customdata[0]}<br><b>Registered Users:</b> %{x:,}<extra></extra>")
        fig3.update_layout(yaxis={'categoryorder': 'total ascending'}, title_x=0.5, width=900, height=500)
        st.plotly_chart(fig3, use_container_width=True)
        with st.expander('View Data'):
            st.dataframe(df3[['State','District','TotalRegisteredUsers']].reset_index(drop=True))
    else:
        st.warning("No data found for Top Districts.")
else:
    st.info("Please select a Year for Top Districts analysis.")
add_vertical_space(2)


# --- 4. App Opens Density Map (Density Mapbox) ---
st.subheader(':blue[Number of App Opens by District (Density)]')
col4a, col4b, buff4 = st.columns([1, 1, 4])
year4 = col4a.selectbox('Year', options=years, key='year4_density_pg3')
quarter4 = col4b.selectbox("Quarter", options=quarter_options, key='quarter4_density_pg3')

if coords_df is not None and geojson_data is not None and year4:
    with st.spinner(f"Loading App Opens density data ({year4} Q{quarter4})..."):
        query4 = f"SELECT State, District, SUM(AppOpens) as TotalAppOpens, Quarter FROM map_user WHERE Year = {year4}"
        if quarter4 != 'All':
            query4 += f" AND Quarter = {int(quarter4)}"
        query4 += " GROUP BY State, District, Quarter HAVING TotalAppOpens > 0"
        df4_user = fetch_data(query4)

    if not df4_user.empty:
        df4_user['District_Lower'] = df4_user['District'].astype(str).str.lower().str.strip()
        df4_merged = pd.merge(df4_user, coords_df, on='District_Lower', how='left')
        df4_merged.dropna(subset=['lat', 'lon'], inplace=True)

        if not df4_merged.empty:
            if quarter4 == 'All':
                df4_plot = df4_merged.groupby(['State', 'District', 'District_Lower', 'lat', 'lon']).agg(
                    TotalAppOpens=('TotalAppOpens', 'sum')
                ).reset_index()
                df4_plot['Quarter'] = 'All'
            else:
                df4_plot = df4_merged

            fig4 = px.density_mapbox(
                df4_plot, lat='lat', lon='lon', z='TotalAppOpens', radius=15,
                center=dict(lat=20.5937, lon=78.9629), zoom=3.8,
                hover_name='District',
                # --- THIS IS THE FIX ---
                hover_data={"State": True,
                            "Quarter": True,
                            "TotalAppOpens": ':,',
                            'lat': False, 'lon': False, 'District_Lower': False
                            # Removed 'z': False
                        },
                # --- END OF FIX ---
                mapbox_style="carto-darkmatter",
                opacity=0.7, labels={'TotalAppOpens': 'Total App Opens', 'z': 'App Opens Density'},
                title=f"App Opens Density ({year4}{f', Q{quarter4}' if quarter4 != 'All' else ''})",
                color_continuous_scale='Blues'
            )
            if geojson_data:
                fig4.update_layout(
                    mapbox_layers=[{
                        "sourcetype": "geojson", "source": geojson_data,
                        "type": "line", "color": "rgba(255,255,255,0.3)",
                        "line": {"width": 0.5}
                    }]
                )
            fig4.update_layout(margin=dict(l=0, r=0, t=40, b=0), width=900, height=500, title_x=0.5)

            st.plotly_chart(fig4, use_container_width=True)
            with st.expander('View Mapped Data'):
                st.dataframe(df4_merged[['State', 'District', 'Quarter', 'TotalAppOpens', 'lat', 'lon']].reset_index(drop=True))
        else:
            st.warning("No districts with App Opens could be mapped.")
    else:
        st.warning("No App Opens data found for selected filters.")
elif not year4:
    st.info("Please select a Year for App Opens Density analysis.")
else:
    # Error/warning for coords/geojson already handled
    pass