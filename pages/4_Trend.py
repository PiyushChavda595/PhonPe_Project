# pages/4_Trend.py
import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px
import altair as alt # Use Altair for bar charts like reference
from streamlit_extras.add_vertical_space import add_vertical_space

# --- Page Config ---
st.set_page_config(page_title='PhonePe Pulse | Trends', layout='wide', page_icon='Logo.png')

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
        conn.close()
        return df
    except mysql.connector.Error as err:
        st.error(f"Database Error: {err}")
        return pd.DataFrame()

# --- Hide elements ---
st.markdown("""<style> footer {visibility: hidden;} </style>""", unsafe_allow_html=True)
st.markdown("""<style>.css-1jc7ptx, .e1ewe7hr3, .viewerBadge_container__1QSob, .styles_viewerBadge__1yB5_, .viewerBadge_link__1S137, .viewerBadge_text__1JaDK {display: none;}</style>""", unsafe_allow_html=True)

st.title(':violet[Trend Analysis]')
add_vertical_space(2)

# --- Fetch Initial Data for Filters ---
with st.spinner("Loading filter options..."):
    # Using map_transaction as it has State, District, Year, Quarter
    states_df = fetch_data("SELECT DISTINCT State FROM map_transaction ORDER BY State")
    years_df = fetch_data("SELECT DISTINCT Year FROM map_transaction ORDER BY Year DESC")
    quarters_df = fetch_data("SELECT DISTINCT Quarter FROM map_transaction ORDER BY Quarter")

states = states_df['State'].tolist() if not states_df.empty else []
years = years_df['Year'].tolist() if not years_df.empty else []
year_options_all = ['All'] + years
quarters = quarters_df['Quarter'].tolist() if not quarters_df.empty else []
quarter_options_all = ["All"] + quarters

# --- 1. Transaction Trend Over Time (Line Charts) ---
st.subheader(':blue[Transaction Trend - Count & Amount]')
add_vertical_space(1)
col1a, col1b, col1c = st.columns(3)
state1 = col1a.selectbox('State', states, key='state1_trend_pg4')
# Fetch districts dynamically
with st.spinner(f"Loading districts for {state1}..."):
    districts_in_state_df = fetch_data(f"SELECT DISTINCT District FROM map_transaction WHERE State = '{state1}' ORDER BY District")
districts1_options = districts_in_state_df['District'].tolist() if not districts_in_state_df.empty else []
district1 = col1b.selectbox('District', districts1_options, key='district1_trend_pg4')
year1 = col1c.selectbox('Year', year_options_all, key='year1_trend_pg4')

if state1 and district1: # Ensure selections are made
    with st.spinner(f"Loading trend data for {district1}, {state1}..."):
        query1 = f"SELECT Year, Quarter, SUM(Transaction_count) as TotalCount, SUM(Transaction_amount) as TotalAmount FROM map_transaction WHERE State = '{state1}' AND District = '{district1}'"
        if year1 != 'All':
            query1 += f" AND Year = {year1}"
        query1 += " GROUP BY Year, Quarter ORDER BY Year, Quarter"
        df1 = fetch_data(query1)

    if not df1.empty:
        df1['Period'] = df1['Year'].astype(str) + '-Q' + df1['Quarter'].astype(str)
        # Sort by Period to ensure lines connect correctly if multiple years shown
        df1 = df1.sort_values(by=['Year', 'Quarter'])

        fig1_count = px.line(df1, x='Period', y='TotalCount', markers=True,
                            title=f'Transaction Count Trend in {district1}, {state1}')
        # Improved hover template
        fig1_count.update_traces(hovertemplate="<b>Period:</b> %{x}<br><b>Count:</b> %{y:,}<extra></extra>")
        fig1_count.update_layout(yaxis_title='Transaction Count', xaxis_title='Period (Year-Quarter)', width=900, height=450, title_x=0.5)

        fig1_amount = px.line(df1, x='Period', y='TotalAmount', markers=True,
                            title=f'Transaction Amount Trend in {district1}, {state1}')
        # Improved hover template
        fig1_amount.update_traces(hovertemplate="<b>Period:</b> %{x}<br><b>Amount:</b> ₹%{y:,.0f}<extra></extra>")
        fig1_amount.update_layout(yaxis_title='Transaction Amount (₹)', xaxis_title='Period (Year-Quarter)', width=900, height=450, title_x=0.5)

        tab1_count, tab1_amount = st.tabs(['🫰 Transaction Count Trend', '💰 Transaction Amount Trend'])
        with tab1_count:
            st.plotly_chart(fig1_count, use_container_width=True) # Use container width
            with st.expander("View Count Data"):
                st.dataframe(df1[['Year', 'Quarter', 'TotalCount']].reset_index(drop=True))
        with tab1_amount:
            st.plotly_chart(fig1_amount, use_container_width=True) # Use container width
            with st.expander("View Amount Data"):
                st.dataframe(df1[['Year', 'Quarter', 'TotalAmount']].reset_index(drop=True))
    else:
        st.warning("No data found for the selected location and year.")
else:
    st.info("Please select a State and District.")
add_vertical_space(2)


# --- 2. Top Categories by Transaction Amount (Altair Bar Charts) ---
st.subheader(':blue[Top Categories by Transaction Volume]')
col2a, col2b, col2c = st.columns([1, 1, 1])
category2 = col2a.selectbox('Category', ('States', 'Districts', 'Pincodes'), key='cat2_trend_pg4')
year2 = col2b.selectbox('Year', years, key='year2_trend_pg4') # Year specific required
quarter2 = col2c.selectbox('Quarter', quarter_options_all, key='quarter2_trend_pg4')

if year2: # Ensure year is selected
    entity = 'State' if category2 == 'States' else ('District' if category2 == 'Districts' else 'Pincode')
    # Determine the correct table and grouping
    if category2 == 'Pincodes':
        table_prefix = 'top'
        group_by_cols = [entity, 'State'] # Include State for Pincode grouping and tooltip
        select_cols = [entity, 'State', 'SUM(Transaction_amount) as TotalAmount']
    elif category2 == 'Districts':
        table_prefix = 'map'
        group_by_cols = [entity, 'State']
        select_cols = [entity, 'State', 'SUM(Transaction_amount) as TotalAmount']
    else: # States
        table_prefix = 'map'
        group_by_cols = [entity]
        select_cols = [entity, 'SUM(Transaction_amount) as TotalAmount']

    with st.spinner(f"Loading top {category2} data..."):
        query2 = f"SELECT {', '.join(select_cols)} FROM {table_prefix}_transaction WHERE Year = {year2}"
        if quarter2 != 'All':
            query2 += f" AND Quarter = {int(quarter2)}"
        query2 += f" GROUP BY {', '.join(group_by_cols)} ORDER BY TotalAmount DESC LIMIT 10"
        df2 = fetch_data(query2)

    if not df2.empty:
        if entity == 'Pincode':
            df2['Pincode'] = df2['Pincode'].astype(str)

        # Base chart definition
        base = alt.Chart(df2, height=500).encode(
            x=alt.X('TotalAmount', title='Total Transaction Amount', axis=alt.Axis(format='~s')),
            y=alt.Y(entity, sort='-x', title=category2[:-1]),
            # Define tooltip fields
            tooltip = [
                alt.Tooltip(entity, title=category2[:-1]), # Title matches axis
                alt.Tooltip('State', title='State') if 'State' in df2.columns else alt.value(None), # Show State if available
                alt.Tooltip('TotalAmount', title='Total Amount', format='~s') # Format amount
            ]
        )

        # Apply coloring
        if 'State' in df2.columns and entity != 'State':
            chart2 = base.mark_bar().encode(color=alt.Color('State', title='State')) # Color by State
        else:
            chart2 = base.mark_bar(color='steelblue') # Single color

        # Add title and make interactive
        chart2 = chart2.properties(
            title=f"Top 10 {category2} by Transaction Amount ({year2}{f', Q{quarter2}' if quarter2 != 'All' else ''})"
        ).configure_title(
            align='center', anchor='middle'
        ).interactive()

        st.altair_chart(chart2, use_container_width=True)
        with st.expander("View Top 10 Data"):
            st.dataframe(df2.reset_index(drop=True))
    else:
        st.warning("No data found for the selected filters.")
else:
    st.info("Please select a Year for Top Categories analysis.")
