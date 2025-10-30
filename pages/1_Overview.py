# pages/1_Overview.py
import pandas as pd
import streamlit as st
import mysql.connector
import plotly.express as px
import json
import os
from streamlit_extras.add_vertical_space import add_vertical_space
from ydata_profiling import ProfileReport # Import for profiling

# --- Page Config ---
st.set_page_config(page_title='PhonePe Pulse | Overview', layout='wide', page_icon='Logo.png')

# --- NEW DB Credentials (using Streamlit Secrets) ---
DB_HOST = st.secrets["database"]["host"]
DB_PORT = st.secrets["database"]["port"]
DB_USER = st.secrets["database"]["user"]
DB_PASSWORD = st.secrets["database"]["password"]
DB_NAME = st.secrets["database"]["db_name"]
DB_SSL_CA = st.secrets["database"]["ssl_ca"]
GEOJSON_FILE = "india_states.geojson" # State-level map needed

# --- DB Fetch Function ---
@st.cache_data(ttl=3600)
def fetch_data(query):
    # The spinner should be outside this function where it's called
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

# --- Load GeoJSON ---
@st.cache_data # Cache GeoJSON loading
def load_geojson(file_path):
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error(f"GeoJSON file '{file_path}' not found. Map cannot be displayed.")
        return None
    except Exception as e:
        st.error(f"Error loading GeoJSON file '{file_path}': {e}")
        return None
geojson_data = load_geojson(GEOJSON_FILE)


# --- State Name Correction ---
state_name_mapping = {
    'Andaman & Nicobar Islands': 'Andaman & Nicobar',
    'Dadra & Nagar Haveli & Daman & Diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'Delhi': 'National Capital Territory of Delhi',
    'Jammu & Kashmir': 'Jammu & Kashmir',
    'Ladakh': 'Ladakh',
    'Telengana': 'Telangana'
}

# --- Hide elements ---
st.markdown("""<style> footer {visibility: hidden;} </style>""", unsafe_allow_html=True)
st.markdown("""<style>.css-1jc7ptx, .e1ewe7hr3, .viewerBadge_container__1QSob, .styles_viewerBadge__1yB5_, .viewerBadge_link__1S137, .viewerBadge_text__1JaDK {display: none;}</style>""", unsafe_allow_html=True)

st.title(':violet[Overview & Data Profiling]')
add_vertical_space(1)

# --- Tab Structure ---
tab_charts, tab_profile = st.tabs(["Quick Visuals", "Detailed Profiling Report"])

with tab_charts:
    st.header("Quick Visual Summaries")
    add_vertical_space(1)

    # --- Fetch Data Needed ---
    # Fetch only necessary columns for performance
    df_agg_trans = fetch_data("SELECT State, Transaction_type, Transaction_count FROM aggregated_transaction")
    df_map_trans = fetch_data("SELECT State, District, Transaction_count FROM map_transaction")
    df_map_user = fetch_data("SELECT State, SUM(RegisteredUsers) as TotalRegisteredUsers FROM map_user GROUP BY State")

    # --- Charts (similar to before) ---
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Transaction Breakdown by Type")
        if not df_agg_trans.empty:
            trans_type_count = df_agg_trans.groupby('Transaction_type')['Transaction_count'].sum().reset_index()
            fig_type = px.pie(trans_type_count, names='Transaction_type', values='Transaction_count', hole=.4, title="Overall Share (Count)")
            fig_type.update_layout(height=400, title_x=0.5, legend_title_text='Type')
            st.plotly_chart(fig_type, use_container_width=True)
        else: 
            st.warning("Aggregated transaction data unavailable.")

        st.subheader("Top 10 Districts (Count)")
        if not df_map_trans.empty:
            trans_district = df_map_trans.groupby(['State', 'District'])['Transaction_count'].sum().reset_index()
            trans_district_sorted = trans_district.sort_values(by='Transaction_count', ascending=False).head(10)
            fig_district = px.bar(trans_district_sorted, x='Transaction_count', y='District', orientation='h', text_auto='.2s',
                                labels={'Transaction_count': "Total Count"}, title="Top 10 Districts", hover_name='State')
            fig_district.update_layout(yaxis=dict(autorange="reversed"), height=400, title_x=0.5)
            st.plotly_chart(fig_district, use_container_width=True)
        else: 
            st.warning("Map transaction data unavailable.")

    with col2:
        st.subheader("Top 10 States (Count)")
        if not df_agg_trans.empty:
            trans_state = df_agg_trans.groupby('State')['Transaction_count'].sum().reset_index()
            trans_state_sorted = trans_state.sort_values(by='Transaction_count', ascending=False).head(10)
            fig_state = px.bar(trans_state_sorted, x='Transaction_count', y='State', orientation='h', text_auto='.2s',
                            labels={'Transaction_count': "Total Count"}, title="Top 10 States")
            fig_state.update_layout(yaxis=dict(autorange="reversed"), height=400, title_x=0.5)
            st.plotly_chart(fig_state, use_container_width=True)
        else: 
            st.warning("Aggregated transaction data unavailable.")

        st.subheader('Registered Users by State')
        if not df_map_user.empty and geojson_data is not None:
            df_map_user['State_Mapped'] = df_map_user['State'].replace(state_name_mapping)
            fig_user_map = px.choropleth(df_map_user, geojson=geojson_data, locations='State_Mapped', featureidkey='properties.st_nm',
                                        color='TotalRegisteredUsers', projection='mercator', labels={'TotalRegisteredUsers': "Registered Users"},
                                        color_continuous_scale='Reds', title="Registered Users Distribution")
            fig_user_map.update_geos(fitbounds='locations', visible=False)
            fig_user_map.update_layout(height=400, title_x=0.5, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig_user_map, use_container_width=True)
        elif geojson_data is None: 
            st.warning("GeoJSON missing.")
        else: 
            st.warning("Map user data unavailable.")


with tab_profile:
    st.header("Detailed Dataset Profiling")
    add_vertical_space(1)
    dataset_options_profile = {
        "Aggregated Transactions": "aggregated_transaction", "Aggregated Users": "aggregated_user",
        "Map Transactions": "map_transaction", "Map Users": "map_user",
        "Top Transactions": "top_transaction", "Top Users": "top_user",
        "Aggregated Insurance": "aggregated_insurance", "Map Insurance": "map_insurance",
        "Top Insurance": "top_insurance"
    }
    selected_profile_name = st.selectbox("Select Dataset to Profile:", dataset_options_profile.keys(), key='profile_select')
    profile_table_name = dataset_options_profile[selected_profile_name]

    if st.button("Generate Profile Report", key=f"gen_{profile_table_name}"):
        df_profile = fetch_data(f"SELECT * FROM {profile_table_name}")
        if not df_profile.empty:
            with st.spinner(f"Generating profile for '{selected_profile_name}'..."):
                profile = ProfileReport(
                    df_profile,
                    title=f"Profiling Report - {selected_profile_name}",
                    explorative=True,
                    minimal=False # Generate full report here
                )
                # Use components.html to display
                st.components.v1.html(profile.to_html(), height=800, scrolling=True)
        else:
            st.error(f"Could not fetch data for {selected_profile_name} to generate report.")
