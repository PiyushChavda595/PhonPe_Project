# pages/5_Comparison.py
import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px
import seaborn as sns # Use Seaborn for catplot
import matplotlib.pyplot as plt # Needed for Seaborn plots in Streamlit
from streamlit_extras.add_vertical_space import add_vertical_space

# --- Page Config ---
st.set_page_config(page_title='PhonePe Pulse | Comparison', layout='wide', page_icon='Logo.png')

# --- NEW DB Credentials (using Streamlit Secrets) ---
DB_HOST = st.secrets["database"]["host"]
DB_PORT = st.secrets["database"]["port"]
DB_USER = st.secrets["database"]["user"]
DB_PASSWORD = st.secrets["database"]["password"]
DB_NAME = st.secrets["database"]["db_name"]
DB_SSL_CA = st.secrets["database"]["ssl_ca"]

# --- DB Fetch Function ---
@st.cache_data(ttl=3600)
def fetch_data(query):
    # Spinner is outside this function
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
        
        # Convert Amount/Count to numeric, coercing errors (specific to this file)
        if 'Transaction_amount' in df.columns:
            df['Transaction_amount'] = pd.to_numeric(df['Transaction_amount'], errors='coerce').fillna(0)
        if 'Transaction_count' in df.columns:
            df['Transaction_count'] = pd.to_numeric(df['Transaction_count'], errors='coerce').fillna(0)
        
        conn.close()
        return df
    except mysql.connector.Error as err:
        st.error(f"Database Error: {err}")
        return pd.DataFrame()

# --- Hide elements ---
st.markdown("""<style> footer {visibility: hidden;} </style>""", unsafe_allow_html=True)
st.markdown("""<style>.css-1jc7ptx, .e1ewe7hr3, .viewerBadge_container__1QSob, .styles_viewerBadge__1yB5_, .viewerBadge_link__1S137, .viewerBadge_text__1JaDK {display: none;}</style>""", unsafe_allow_html=True)

st.title(':violet[Comparative Analysis]')
add_vertical_space(2)

# --- Fetch Initial Data for Filters & Add Region ---
@st.cache_data(ttl=3600)
def get_all_agg_trans_with_region():
    with st.spinner("Loading base comparison data..."): # Spinner for initial load
        df = fetch_data("SELECT State, Year, Quarter, Transaction_type, Transaction_count, Transaction_amount FROM aggregated_transaction")
    if not df.empty:
        south = ['Andhra Pradesh', 'Karnataka', 'Kerala', 'Tamil Nadu', 'Telangana', 'Puducherry', 'Lakshadweep', 'Andaman & Nicobar Islands']
        central = ['Chhattisgarh', 'Madhya Pradesh', 'Uttar Pradesh', 'Uttarakhand']
        west = ['Goa', 'Gujarat', 'Maharashtra', 'Dadra & Nagar Haveli & Daman & Diu']
        north = ['Chandigarh', 'Delhi', 'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir', 'Ladakh', 'Punjab', 'Rajasthan']
        east = ['Bihar', 'Jharkhand', 'Odisha', 'West Bengal', 'Arunachal Pradesh', 'Assam', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland', 'Sikkim', 'Tripura']
        conditions = [df['State'].isin(south), df['State'].isin(central), df['State'].isin(west), df['State'].isin(north), df['State'].isin(east)]
        choices = ['South', 'Central', 'West', 'North', 'East']
        df['Region'] = pd.Series(pd.NA).astype("string")
        for i, choice in enumerate(choices):
            df['Region'] = df['Region'].fillna(pd.NA).mask(conditions[i], choice)
        df['Region'] = df['Region'].fillna('Unknown')
        df["Transaction_amount(B)"] = df["Transaction_amount"] / 1e9
        df["Year"] = pd.Categorical(df["Year"], categories=sorted(df["Year"].unique()), ordered=True)
    return df

trans_df_all = get_all_agg_trans_with_region()

# Get filter options safely
states = sorted(trans_df_all['State'].unique()) if not trans_df_all.empty else []
years = sorted(trans_df_all['Year'].unique()) if not trans_df_all.empty else []
quarters = sorted(trans_df_all['Quarter'].unique()) if not trans_df_all.empty else []
quarter_options = ["All"] + quarters
regions = sorted(trans_df_all['Region'].dropna().unique()) if not trans_df_all.empty else []


# --- 1. Region-wise Transaction Volume Comparison (Seaborn Catplot) ---
st.subheader(':blue[Region-wise Transaction Amount Comparison (Billions ₹)]')
if not trans_df_all.empty:
    with st.spinner("Generating region comparison chart..."):
        df_region_year = trans_df_all.groupby(['Region', 'Year'], observed=False)['Transaction_amount(B)'].sum().reset_index()

        # Check if data exists after aggregation
        if not df_region_year.empty:
            sns.set_style("whitegrid")
            try:
                fig1 = sns.catplot(
                    x="Year", y="Transaction_amount(B)",
                    col="Region", data=df_region_year,
                    kind="bar", errorbar=None,
                    height=4, aspect=1.2, col_wrap=3,
                    sharex=True, sharey=False
                )
                fig1.set_axis_labels("Year", "Total Transaction Amount (Billions ₹)")
                fig1.set_titles("Region: {col_name}")
                fig1.fig.suptitle('Total Transaction Amount per Year by Region', y=1.03)
                st.pyplot(fig1)
            except Exception as e:
                st.error(f"Error generating Seaborn plot: {e}")
        else:
            st.warning("No data found after grouping by Region and Year.")
else:
    st.warning("No base data available for region comparison.")
add_vertical_space(2)

# --- 2. State Comparison by Transaction Type (Plotly Grouped Bar) ---
st.subheader(':blue[State Comparison by Transaction Type (Count)]')
col2a, col2b, col2c = st.columns([2, 1, 1])
selected_states = col2a.multiselect("Select States to Compare", states, key='states_compare_pg5')
year2 = col2b.selectbox("Year", years, key='year2_compare_pg5')
quarter2 = col2c.selectbox("Quarter", quarter_options, key='quarter2_compare_pg5')

if selected_states and year2: # Ensure state(s) and year selected
    with st.spinner("Loading state comparison data..."):
        df2_filtered = trans_df_all[
            (trans_df_all["State"].isin(selected_states)) &
            (trans_df_all["Year"] == year2)
        ]
        if quarter2 != "All":
            df2_filtered = df2_filtered[df2_filtered["Quarter"] == int(quarter2)]

    if not df2_filtered.empty:
        df2_grouped = df2_filtered.groupby(['State', 'Transaction_type'])['Transaction_count'].sum().reset_index()

        fig2 = px.bar(
            df2_grouped, x="Transaction_type", y="Transaction_count",
            color="State", barmode='group',
            title=f"Transaction Count Comparison ({year2}{f', Q{quarter2}' if quarter2 != 'All' else ''})",
            labels={'Transaction_count': 'Total Transaction Count', 'Transaction_type': 'Transaction Type'}
        )
        # Improved hover template
        fig2.update_traces(hovertemplate="<b>State:</b> %{fullData.name}<br><b>Type:</b> %{x}<br><b>Count:</b> %{y:,}<extra></extra>")
        fig2.update_layout(width=900, height=500, title_x=0.5)
        fig2.update_traces(marker_line=dict(width=1, color='DarkSlateGrey'))
        st.plotly_chart(fig2, use_container_width=True) # Use container width
        with st.expander("View Comparison Data"):
            st.dataframe(df2_grouped)
    else:
        st.warning("No data found for the selected states and period.")
elif not selected_states:
    st.info("Select at least one state to compare.")
else: # Year not selected
    st.info("Select a Year to compare states.")
add_vertical_space(2)

# --- 3. Quarter-wise Transaction Amount Comparison (Plotly Pie Chart) ---
st.subheader(':blue[Quarterly Transaction Amount Share by Region]')
col3a, col3b, buff3 = st.columns([1, 1, 3])
region3 = col3a.selectbox('Region', regions, key='region3_pie_pg5')
year3 = col3b.selectbox('Year', years, key='year3_pie_pg5')

if region3 and year3: # Ensure selections
    with st.spinner(f"Loading quarterly data for {region3} ({year3})..."):
        df3_filtered = trans_df_all[(trans_df_all['Region'] == region3) & (trans_df_all['Year'] == year3)]

    if not df3_filtered.empty:
        df3_grouped = df3_filtered.groupby('Quarter')['Transaction_amount(B)'].sum().reset_index()
        # Add check if sum is zero before plotting pie
        if df3_grouped['Transaction_amount(B)'].sum() > 0:
            df3_grouped['Quarter_Label'] = 'Q' + df3_grouped['Quarter'].astype(str)

            fig3 = px.pie(
                df3_grouped, values='Transaction_amount(B)', names='Quarter_Label',
                title=f'Quarterly Share of Transaction Amount (B ₹) in {region3} ({year3})'
            )
            # Improved hover template for Pie
            fig3.update_traces(hovertemplate="<b>Quarter:</b> %{label}<br><b>Amount (B):</b> %{value:.2f}<br><b>Share:</b> %{percent}<extra></extra>",
                            textposition='inside', textinfo='percent+label')
            fig3.update_layout(width=800, height=500, title_x=0.5)
            st.plotly_chart(fig3, use_container_width=True) # Use container width
            with st.expander("View Quarterly Data"):
                st.dataframe(df3_grouped[['Quarter_Label', 'Transaction_amount(B)']].reset_index(drop=True))
        else:
            st.warning(f"Total transaction amount is zero for {region3} in {year3}. Cannot display pie chart.")
    else:
        st.warning("No data found for the selected region and year.")
else:
    st.info("Please select a Region and Year.")
