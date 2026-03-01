"""
The Migration Marketing Dashboard - Combined GSC + Pipeline + Contacts
Combined script with GSC data, Pipeline Analysis, Contact Analysis, and Consultant Capacity
"""

import os
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

# Plotly
import plotly.express as px
import plotly.graph_objects as go

# Meta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign

# GA4
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest
)

# GSC
from googleapiclient.discovery import build
from google.oauth2 import service_account

# --- GSC CONFIGURATION ---
GSC_SITE_URL = "https://themigration.com.au/"
GSC_KEY_FILE = r"C:\Users\Hafsa Saleh\IdeaProjects\Themigration\google_creds.json"
PROPERTY_ID = "354763938"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json"

# --- DASHBOARD SETUP ---
ACCESS_TOKEN = "EAAWYAtm7TKsBQ9Ehq3GU9NVflXbF4r370ll5H3uTNoum8aF6NntVzhWOejHrg0ecPZBLV6i1077Vy3TYZBWrcyJKBZAjbSiMpUmZAgsg38UGuxrDSyytU6CqfZCnY2rjLDODIzpdwC7e6qpGaSutqxYgmSgwKsRcruGmxkbuApTQuwYezPkQb3lMmLZAAWLcFZCZA8KI"
APP_ID = "1574512893840555"
APP_SECRET = "2f2984631ab5a1dd0606a8d09e45f100" 
AD_ACCOUNT_ID = "act_600555439172695"

if "theme_choice" not in st.session_state:
    st.session_state.theme_choice = "Light Reflection"

# --- THEME DYNAMICS (Based on dashboard_new.html) ---
if st.session_state.theme_choice == "Dark Intelligence":
    bg_color = "#0f1419"
    surface_color = "#1a1c1e"
    text_color = "#e8edf5"
    secondary_text = "#94a3b8"
    sidebar_bg = "#1a1c1e"
    plotly_template = "plotly_dark"
    chart_bg = "#1a1c1e"
    accent = "#e2b714"
    border_color = "#2d2f31"
    table_bg = "#1a1c1e"
    btn_bg = "#ffffff"
    btn_text = "#000000"
    input_bg = "#2d2f31"
    input_text = "#ffffff"
else:
    bg_color = "#f8fafc"
    surface_color = "#ffffff"
    text_color = "#1e293b"
    secondary_text = "#64748b"
    sidebar_bg = "#1a1c1e"
    plotly_template = "plotly_white"
    chart_bg = "#ffffff"
    accent = "#e2b714"
    border_color = "#e2e8f0"
    btn_bg = "#000000"
    btn_text = "#ffffff"
    input_bg = "#2d2f31"
    input_text = "#ffffff"
    table_bg = "#ffffff"

# --- HELPER FUNCTIONS ---
def okr_scorecard(label, value, color="#00b4d8"):
    st.markdown(f"""
        <div class="okr-card" style="border-left: 5px solid {color}; font-family: 'Helvetica', 'Arial', sans-serif;">
            <div class="okr-val" style="color: {color}; font-family: 'Helvetica', 'Arial', sans-serif; font-size: 38px; font-weight: bold; margin-bottom: 5px;">{value}</div>
            <div class="okr-label" style="font-family: 'Helvetica', 'Arial', sans-serif; font-size: 12px; font-weight: 700; text-transform: uppercase; color: {secondary_text}; letter-spacing: 1px;">{label}</div>
        </div>
    """, unsafe_allow_html=True)

def convert_stage_no_to_percentage(stage_no_str):
    """Convert Stage No (e.g., '1 / 11') to percentage"""
    try:
        if pd.isna(stage_no_str) or stage_no_str is None:
            return None
        if isinstance(stage_no_str, str):
            parts = stage_no_str.split('/')
            if len(parts) == 2:
                current = float(parts[0].strip())
                total = float(parts[1].strip())
                if total > 0:
                    return (current / total) * 100
        return None
    except:
        return None

# L2C Education Stage Order
STAGE_ORDER = [
    "New Lead", "Qualifier", "Pre Sales (1)", "Pre Sales (2)",
    "Booking Link Shared", "Appointment Booked", "Post Consultation",
    "No Show", "Initial Requested", "Initial Received", "COE Received", "Won"
]

# --- ADVANCED CSS ---
st.markdown(f"""
    <style>
    html, body, [data-testid="stAppViewContainer"], [data-testid="stHeader"], .stMarkdown {{
        background-color: {bg_color} !important;
        color: {text_color} !important;
        font-family: 'Helvetica', 'Arial', sans-serif !important;
    }}
    .block-container {{ max-width: 100% !important; padding-left: 2rem; padding-right: 2rem; }}
    [data-testid="stSidebar"] {{ background-color: {sidebar_bg} !important; }}
    [data-testid="stSidebar"] *, [data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {{ color: white !important; font-family: 'Helvetica', 'Arial', sans-serif !important; }}
    .okr-card {{ background-color: {surface_color}; padding: 25px 15px; border-radius: 4px; text-align: center; box-shadow: 0 4px 10px rgba(0,0,0,0.15); margin-bottom: 15px; }}
    .brand-header {{ background: linear-gradient(90deg, #00607a 0%, #0084a8 100%); padding: 30px; border-radius: 12px; color: white !important; text-align: center; margin-bottom: 25px; }}
    .brand-header h1 {{ color: white !important; font-size: 32px; font-weight: bold; }}
    div[data-baseweb="tab-list"] button p {{ color: {text_color} !important; }}
    div[data-baseweb="tab-list"] button[aria-selected="true"] p {{ color: #005a84 !important; font-weight: bold !important; }}
    [data-testid="stDataFrame"], [data-testid="stDataFrame"] > div {{ background-color: {table_bg} !important; }}
    [data-testid="stDataFrame"] div[role="grid"] {{ background-color: {table_bg} !important; }}
    [data-testid="stDataFrame"] div[role="columnheader"] p, [data-testid="stDataFrame"] div[role="cell"] p, [data-testid="stDataFrame"] div[role="grid"] p {{ color: #ffffff !important; }}
    .stTable {{ background-color: {table_bg} !important; color: #ffffff !important; }}
    .stTable td, .stTable th {{ background-color: {table_bg} !important; color: #ffffff !important; border: 1px solid {border_color} !important; }}
    ::-webkit-scrollbar {{ background-color: {table_bg}; width: 8px; }}
    ::-webkit-scrollbar-thumb {{ background-color: {border_color}; border-radius: 4px; }}
    </style>
""", unsafe_allow_html=True)

# --- DATA LOADING FUNCTIONS ---
@st.cache_data(ttl=600)
def load_pipeline_data():
    """Load opportunities data from local CSV"""
    data = {'opportunities': pd.DataFrame()}
    opps_file = "ghl/applications_tab_2025-11-01_to_2026-02-26.csv"
    if os.path.exists(opps_file):
        df = pd.read_csv(opps_file)
        if 'Created on (AEDT)' in df.columns:
            df['Created Date'] = pd.to_datetime(df['Created on (AEDT)'], errors='coerce').dt.date
        if 'Stage No' in df.columns:
            df['Stage Percentage'] = df['Stage No'].apply(convert_stage_no_to_percentage)
        data['opportunities'] = df
        print(f"Loaded {len(df)} opportunities from file")
    return data

@st.cache_data(ttl=600)
def load_contact_data():
    """Load contacts data from local CSV"""
    data = {'contacts': pd.DataFrame()}
    contacts_file = "ghl/contact_2025-11-01_to_2026-02-26.csv"
    if os.path.exists(contacts_file):
        df = pd.read_csv(contacts_file)
        if 'Created (AEDT)' in df.columns:
            df['Created Date'] = pd.to_datetime(df['Created (AEDT)'], errors='coerce').dt.date
        data['contacts'] = df
        print(f"Loaded {len(df)} contacts from file")
    return data

@st.cache_data(ttl=600)
def load_consultant_data():
    """Load consultant capacity data"""
    data = {'consultants_today': pd.DataFrame(), 'consultants_weekly': pd.DataFrame()}
    consult_today = "ghl/consultant_capacity_today.csv"
    if os.path.exists(consult_today):
        data['consultants_today'] = pd.read_csv(consult_today)
    consult_weekly = "ghl/consultant_capacity_weekly.csv"
    if os.path.exists(consult_weekly):
        data['consultants_weekly'] = pd.read_csv(consult_weekly)
    return data

# --- GSC FUNCTIONS ---
@st.cache_data(ttl=600)
def fetch_meta_data(start, end):
    try:
        FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)
        account = AdAccount(AD_ACCOUNT_ID)
        fields = ['campaign_id', 'campaign_name', 'objective', 'reach', 'frequency', 'impressions', 'spend', 'cpm', 'clicks', 'ctr', 'cpc', 'inline_link_clicks', 'inline_link_click_ctr', 'actions', 'action_values', 'cost_per_action_type', 'video_thruplay_watched_actions']
        params = {'level': 'campaign', 'time_range': {'since': start.strftime('%Y-%m-%d'), 'until': end.strftime('%Y-%m-%d')}, 'filtering': [{'field': 'campaign.effective_status', 'operator': 'IN', 'value': ['ACTIVE', 'PAUSED']}]}
        insights = account.get_insights(fields=fields, params=params)
        data = []
        for entry in insights:
            actions = entry.get('actions', [])
            video_actions = entry.get('video_thruplay_watched_actions', [])
            costs = entry.get('cost_per_action_type', [])
            
            def get_thruplays(actions):
                for act in actions:
                    if act['action_type'] in ['video_view_15_sec', 'video_played_to_completion']:
                        return act['value']
                return 0
            
            def get_act(name): return sum(float(a['value']) for a in actions if a['action_type'] == name)
            def get_cost(name): return next((float(a['value']) for a in costs if a['action_type'] == name), 0)
            
            camp_id = entry.get('campaign_id')
            campaign_obj = Campaign(camp_id).api_get(fields=['daily_budget', 'lifetime_budget'])
            raw_budget = campaign_obj.get('daily_budget') or campaign_obj.get('lifetime_budget') or 0
            
            lead_forms = get_act('lead')
            web_conversions = get_act('offsite_conversion.fb_pixel_submit_application') + get_act('offsite_conversion.fb_pixel_lead') + get_act('offsite_conversion.fb_pixel_purchase')
            if web_conversions == 0 and lead_forms == 0:
                web_conversions = sum(float(a['value']) for a in actions if 'offsite_conversion' in a['action_type'])
            results_total = lead_forms + web_conversions
            final_results = int(lead_forms if lead_forms > 0 else results_total)
            
            def get_video_act(name='thruplay'):
                total = 0
                target_keys = [name, f'video_{name}', 'video_thruplay', 'thruplay', 'video_thruplays', 'video_view_15_sec', 'video_played_to_completion']
                for a in actions:
                    if a.get('action_type', '').lower() in [k.lower() for k in target_keys]:
                        total += float(a.get('value') or 0)
                for v in video_actions:
                    if v.get('action_type', '').lower() in [k.lower() for k in target_keys]:
                        total += float(v.get('value') or 0)
                return total
            
            data.append({
                'Campaign': entry.get('campaign_name'), 'Results': final_results, 'Reach': entry.get('reach'),
                'Frequency': round(float(entry.get('frequency', 0)), 2), 'Cost per result': f"${get_cost('lead') or get_cost('offsite_conversion.fb_pixel_purchase') or 0:.2f}",
                'Budget': f"${float(raw_budget)/100:.2f}", 'Amount spent': f"${float(entry.get('spend', 0)):.2f}",
                'Impressions': entry.get('impressions'), 'CPM (cost per 1,000 impressions)': f"${float(entry.get('cpm', 0)):.2f}",
                'Link clicks': int(entry.get('inline_link_clicks', 0)), 'CPC (cost per link click)': f"${get_cost('link_click'):.2f}",
                'CTR (link click-through rate)': f"{float(entry.get('inline_link_click_ctr', 0)):.2f}%",
                'Clicks (all)': entry.get('clicks'), 'CTR (all)': f"{float(entry.get('ctr', 0)):.2f}%",
                'CPC (all)': f"${float(entry.get('cpc', 0)):.2f}", 'Landing page views': int(get_act('landing_page_view')),
                'Cost per landing page view': f"${get_cost('landing_page_view'):.2f}",
                'Result rate': f"{(final_results/float(entry.get('impressions', 1)))*100:.2f}%" if float(entry.get('impressions', 0)) > 0 else "0.00%",
                'Views': int(get_act('video_view')), '3 sec video plays': int(get_act('video_view')),
                'Thruplays': int(get_video_act()), 'Instagram profile visits': int(get_act('instagram_profile_visit')),
                'Facebook likes': int(get_act('like')), 'Cost per like': f"${get_cost('like'):.2f}",
                'Instagram follows': int(get_act('instagram_profile_follower_add')), 'Cost per page engagment': f"${get_cost('page_engagement'):.2f}",
                'Post engagement': int(get_act('post_engagement')), 'Post reactions': int(get_act('post_reaction'))
            })
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error mirroring live Meta data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def fetch_ga4_data(start, end):
    try:
        client = BetaAnalyticsDataClient()
        main_request = RunReportRequest(property=f"properties/{PROPERTY_ID}", dimensions=[Dimension(name="date")],
            metrics=[Metric(name="activeUsers"), Metric(name="sessions"), Metric(name="averageSessionDuration"),
            Metric(name="bounceRate"), Metric(name="newUsers"), Metric(name="totalUsers"), Metric(name="screenPageViews")],
            date_ranges=[DateRange(start_date=start.strftime('%Y-%m-%d'), end_date=end.strftime('%Y-%m-%d'))])
        channel_request = RunReportRequest(property=f"properties/{PROPERTY_ID}", dimensions=[Dimension(name="sessionDefaultChannelGroup")],
            metrics=[Metric(name="sessions"), Metric(name="keyEvents")],
            date_ranges=[DateRange(start_date=start.strftime('%Y-%m-%d'), end_date=end.strftime('%Y-%m-%d'))])
        geo_request = RunReportRequest(property=f"properties/{PROPERTY_ID}", dimensions=[Dimension(name="country")],
            metrics=[Metric(name="activeUsers")],
            date_ranges=[DateRange(start_date=start.strftime('%Y-%m-%d'), end_date=end.strftime('%Y-%m-%d'))])
        lp_request = RunReportRequest(property=f"properties/{PROPERTY_ID}", dimensions=[Dimension(name="landingPage")],
            metrics=[Metric(name="sessions"), Metric(name="activeUsers"), Metric(name="newUsers"),
            Metric(name="averageSessionDuration"), Metric(name="keyEvents")],
            date_ranges=[DateRange(start_date=start.strftime('%Y-%m-%d'), end_date=end.strftime('%Y-%m-%d'))], limit=10)
        event_request = RunReportRequest(property=f"properties/{PROPERTY_ID}", dimensions=[Dimension(name="eventName")],
            metrics=[Metric(name="eventCount"), Metric(name="totalUsers")],
            date_ranges=[DateRange(start_date=start.strftime('%Y-%m-%d'), end_date=end.strftime('%Y-%m-%d'))], limit=5)
        return (client.run_report(main_request), client.run_report(channel_request), client.run_report(geo_request),
                client.run_report(lp_request), client.run_report(event_request))
    except Exception as e:
        st.error(f"GA4 Error: {e}")
        return None, None, None, None, None

@st.cache_data(ttl=600)
def fetch_gsc_data(start, end):
    try:
        credentials = service_account.Credentials.from_service_account_file(GSC_KEY_FILE)
        service = build('searchconsole', 'v1', credentials=credentials)
        start_date, end_date = start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')
        trend_request = {'startDate': start_date, 'endDate': end_date, 'dimensions': ['date'], 'rowLimit': 1000}
        query_request = {'startDate': start_date, 'endDate': end_date, 'dimensions': ['query'], 'rowLimit': 10}
        page_request = {'startDate': start_date, 'endDate': end_date, 'dimensions': ['page'], 'rowLimit': 10}
        trend_res = service.searchanalytics().query(siteUrl=GSC_SITE_URL, body=trend_request).execute()
        query_res = service.searchanalytics().query(siteUrl=GSC_SITE_URL, body=query_request).execute()
        page_res = service.searchanalytics().query(siteUrl=GSC_SITE_URL, body=page_request).execute()
        return trend_res.get('rows', []), query_res.get('rows', []), page_res.get('rows', [])
    except Exception as e:
        st.error(f"GSC Error: {e}")
        return [], [], []

# --- SIDEBAR ---
with st.sidebar:
    st.image("https://themigration.com.au/wp-content/uploads/2022/10/The-Migration-Logo.png", use_container_width=True)
    st.markdown("---")
    choice = st.radio("Appearance", ["Light Reflection", "Dark Intelligence"], key="theme_radio_main")
    if choice != st.session_state.theme_choice:
        st.session_state.theme_choice = choice
        st.rerun()
    st.subheader("Date Range")
    date_range = st.date_input("Select Range", [datetime(2025, 11, 1), datetime.now()], key="main_date_range_picker")

# --- MAIN RENDER ---
st.markdown('<div class="brand-header"><h1>The Migration Strategic Dashboard</h1></div>', unsafe_allow_html=True)

# Load all data
pipeline_data = load_pipeline_data()
contact_data = load_contact_data()
consultant_data = load_consultant_data()

# --- TABS ---
tab0, tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🎯 Our Vision", "📊 Ads & Creatives", "📈 Traffic Behaviour", 
    "🔍 SEO Performance", "💼 Pipeline Analysis", "👥 Contact Analysis", "👨‍🏫 Consultant Capacity"
])

# === TAB 0: OUR VISION ===
with tab0:
    st.markdown("### Strategic Alignment")
    c1, c2, c3, c4 = st.columns(4)
    with c1: okr_scorecard("Current Clients", "344")
    with c2: okr_scorecard("Target Clients", "1,900")
    with c3: okr_scorecard("Growth Required", "452%")
    with c4: okr_scorecard("Cultures Connected", "6", color="#f6ad55")
    st.write("##")
    v1, v2 = st.columns(2)
    v1.markdown(f"<div style='border-left: 5px solid #00b4d8; padding: 20px; background: {surface_color}; height: 120px;'><b>VISION</b><br><small>To be the world's most trusted migration partner...</small></div>", unsafe_allow_html=True)
    v2.markdown(f"<div style='border-left: 5px solid #8b5cfc; padding: 20px; background: {surface_color}; height: 120px;'><b>MISSION</b><br><small>Solving migration challenges with transparency and accuracy...</small></div>", unsafe_allow_html=True)

# === TAB 1: ADS & CREATIVES ===
with tab1:
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        df = fetch_meta_data(date_range[0], date_range[1])
        if not df.empty:
            df_calc = df.copy()
            def clean_num(val): return pd.to_numeric(str(val).replace('$', '').replace('%', '').replace(',', ''), errors='coerce')
            numeric_cols = ['Amount spent', 'Results', 'CTR (all)', 'Impressions', 'Link clicks', 'CPC (cost per link click)', 'CTR (link click-through rate)', '3 sec video plays', 'Thruplays']
            for col in numeric_cols:
                if col in df_calc.columns: df_calc[col] = df_calc[col].apply(clean_num).fillna(0)
            
            total_spend = df_calc['Amount spent'].sum()
            total_leads = df_calc['Results'].sum()
            avg_ctr_all = df_calc['CTR (all)'].mean()
            total_impr = df_calc['Impressions'].sum()
            avg_cpl_all = total_spend / total_leads if total_leads > 0 else 0
            
            m_cols = st.columns(5)
            with m_cols[0]: okr_scorecard("Ads Spend", f"${total_spend:,.2f}")
            with m_cols[1]: okr_scorecard("Leads", f"{int(total_leads):,}")
            with m_cols[2]: okr_scorecard("CTR (all)", f"{avg_ctr_all:.2f}%")
            with m_cols[3]: okr_scorecard("CPL (all)", f"${avg_cpl_all:.2f}")
            with m_cols[4]: okr_scorecard("Impressions", f"{int(total_impr):,}")
            
            st.markdown("---")
            total_links = df_calc['Link clicks'].sum()
            avg_cplc = df_calc['CPC (cost per link click)'].mean()
            avg_lctr = df_calc['CTR (link click-through rate)'].mean()
            l_cols = st.columns(3)
            with l_cols[0]: okr_scorecard("Link Clicks", f"{int(total_links):,}", color="#8b5cfc")
            with l_cols[1]: okr_scorecard("Cost per link click", f"${avg_cplc:.2f}", color="#8b5cfc")
            with l_cols[2]: okr_scorecard("CTR (link clicks)", f"{avg_lctr:.2f}%", color="#8b5cfc")
            
            st.markdown("---")
            st.markdown("## Strategic Performance Analysis")
            df_calc['Result Rate (%)'] = (df_calc['Results'] / df_calc['Impressions'] * 100).fillna(0)
            fig_intent = px.scatter(df_calc, x='CTR (link click-through rate)', y='Result Rate (%)', size='Results', color='Campaign', hover_name='Campaign', template=plotly_template, height=400)
            fig_intent.add_hline(y=df_calc['Result Rate (%)'].mean(), line_dash="dot", annotation_text="Avg Result Rate")
            fig_intent.update_layout(paper_bgcolor=chart_bg, plot_bgcolor=chart_bg, font=dict(family="Helvetica, Arial, sans-serif", color=text_color))
            st.plotly_chart(fig_intent, use_container_width=True)
            
            st.markdown("### Campaign Reflection")
            st.dataframe(df, use_container_width=True)
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download Data Reflection (CSV)", data=csv, file_name="meta_report.csv", mime="text/csv")

# === TAB 2: TRAFFIC BEHAVIOUR ===
with tab2:
    main_res, chan_res, geo_res, lp_res, event_res = fetch_ga4_data(date_range[0], date_range[1])
    if main_res and lp_res:
        rows = []
        for row in main_res.rows:
            rows.append({"Date": row.dimension_values[0].value, "Active Users": int(row.metric_values[0].value), "Sessions": int(row.metric_values[1].value),
                "Avg Duration": float(row.metric_values[2].value), "Bounce Rate": float(row.metric_values[3].value),
                "New Users": int(row.metric_values[4].value), "Total Users": int(row.metric_values[5].value)})
        df_main = pd.DataFrame(rows)
        total_active = df_main["Active Users"].sum()
        total_sessions = df_main["Sessions"].sum()
        avg_bounce = df_main["Bounce Rate"].mean()
        s1, s2, s3, s4 = st.columns(4)
        with s1: okr_scorecard("Active Users", f"{total_active:,}")
        with s2: okr_scorecard("Sessions", f"{total_sessions:,}")
        with s3: okr_scorecard("Avg Session Duration", "1m 42s")
        with s4: okr_scorecard("Bounce Rate", f"{avg_bounce * 100:.1f}%")
        
        st.markdown("---")
        total_new = df_main["New Users"].sum()
        total_users = df_main["Total Users"].sum()
        u1, u2, u3 = st.columns(3)
        with u1: okr_scorecard("Total Users", f"{total_users:,}", color="#8b5cfc")
        with u2: okr_scorecard("New Users", f"{total_new:,}", color="#8b5cfc")
        with u3: okr_scorecard("Returning Users", f"{total_users - total_new:,}", color="#8b5cfc")
        
        col_chan, col_geo = st.columns([1, 1])
        with col_chan:
            st.subheader("User Acquisition: Channel Group")
            chan_data = [{"Channel": r.dimension_values[0].value, "Sessions": int(r.metric_values[0].value)} for r in chan_res.rows]
            fig_chan = px.bar(pd.DataFrame(chan_data), x="Sessions", y="Channel", orientation='h', template=plotly_template)
            fig_chan.update_layout(paper_bgcolor=chart_bg, plot_bgcolor=chart_bg, font=dict(family="Helvetica, Arial, sans-serif", color=text_color))
            st.plotly_chart(fig_chan, use_container_width=True)
        
        with col_geo:
            st.subheader("Active Users by Country")
            geo_list = [{"Country": r.dimension_values[0].value, "Users": int(r.metric_values[0].value)} for r in geo_res.rows]
            geo_df = pd.DataFrame(geo_list)
            target_countries = ['Australia', 'Pakistan', 'Bangladesh', 'India', 'China']
            geo_df['Country Group'] = geo_df['Country'].apply(lambda x: x if x in target_countries else 'Others')
            geo_final = geo_df.groupby('Country Group')['Users'].sum().reset_index()
            fig_donut = px.pie(geo_final, values="Users", names="Country Group", hole=0.4, template=plotly_template, color_discrete_sequence=['#3b82f6', '#7c5cfc', '#10b981', '#4b5563'])
            fig_donut.update_layout(paper_bgcolor=chart_bg, plot_bgcolor=chart_bg, font=dict(family="Helvetica, Arial, sans-serif", color=text_color))
            st.plotly_chart(fig_donut, use_container_width=True)

# === TAB 3: SEO PERFORMANCE ===
with tab3:
    st.markdown("### SEO Performance Intelligence")
    gsc_trend, gsc_queries, gsc_pages = fetch_gsc_data(date_range[0], date_range[1])
    if gsc_trend:
        df_trend = pd.DataFrame([{'Date': r['keys'][0], 'Clicks': r['clicks'], 'Impressions': r['impressions'], 'CTR': r['ctr'], 'Position': r['position']} for r in gsc_trend])
        total_clicks = df_trend['Clicks'].sum()
        total_impr = df_trend['Impressions'].sum()
        avg_ctr = (total_clicks / total_impr * 100) if total_impr > 0 else 0
        avg_pos = df_trend['Position'].mean()
        c1, c2, c3, c4 = st.columns(4)
        with c1: okr_scorecard("Total Clicks", f"{int(total_clicks):,}")
        with c2: okr_scorecard("Total Impressions", f"{int(total_impr):,}")
        with c3: okr_scorecard("Avg. CTR", f"{avg_ctr:.2f}%")
        with c4: okr_scorecard("Avg. Position", f"{avg_pos:.1f}")
        
        st.markdown("---")
        fig_gsc = go.Figure()
        fig_gsc.add_trace(go.Scatter(x=df_trend['Date'], y=df_trend['Clicks'], name="Clicks", line=dict(color='#00b4d8', width=3), fill='tozeroy'))
        fig_gsc.add_trace(go.Scatter(x=df_trend['Date'], y=df_trend['Impressions'], name="Impressions", line=dict(color='#8b5cfc', dash='dot'), yaxis="y2"))
        fig_gsc.update_layout(template=plotly_template, paper_bgcolor=chart_bg, plot_bgcolor=chart_bg, font=dict(family="Helvetica, Arial, sans-serif", color=text_color),
            height=400, yaxis=dict(title="Clicks", color='#00b4d8', gridcolor=border_color), yaxis2=dict(title="Impressions", overlaying='y', side='right', color='#8b5cfc', gridcolor=border_color), margin=dict(l=0, r=0, t=20, b=0), hovermode="x unified")
        st.plotly_chart(fig_gsc, use_container_width=True)
        
        col_q, col_p = st.columns(2)
        with col_q:
            st.subheader("Top Search Queries")
            df_queries = pd.DataFrame([{'Keyword': r['keys'][0], 'Clicks': int(r['clicks']), 'Position': round(r['position'], 1)} for r in gsc_queries])
            st.dataframe(df_queries, use_container_width=True, hide_index=True)
        with col_p:
            st.subheader("Your Content Insights")
            df_content = pd.DataFrame([{'Page Path': r['keys'][0].replace(GSC_SITE_URL, '/'), 'Clicks': int(r['clicks']), 'CTR': f"{r['ctr']*100:.1f}%"} for r in gsc_pages])
            st.dataframe(df_content, use_container_width=True, hide_index=True)

# === TAB 4: PIPELINE ANALYSIS ===
with tab4:
    st.header("💼 Pipeline Analysis")
    opps = pipeline_data['opportunities']
    if opps.empty:
        st.warning("No opportunity data available")
    else:
        # Handle date_range - ensure we have date objects
        start_date = date_range[0] if hasattr(date_range[0], 'date') else date_range[0]
        end_date = date_range[1] if hasattr(date_range[1], 'date') else date_range[1]
        
        if 'Created Date' in opps.columns:
            opps_filtered = opps[(opps['Created Date'] >= start_date) & (opps['Created Date'] <= end_date)].copy()
        else:
            opps_filtered = opps.copy()
        
        st.markdown(f"**Filtered Records:** {len(opps_filtered)} opportunities")
        
        # Scorecards
        st.subheader("📊 Scorecards")
        col1, col2, col3 = st.columns(3)
        with col1: okr_scorecard("Total Opportunities", f"{len(opps_filtered):,}", "#3498db")
        with col2: okr_scorecard("Total Value", f"${opps_filtered['Opportunity Value'].sum():,.0f}", "#27ae60") if 'Opportunity Value' in opps_filtered.columns else okr_scorecard("Total Value", "$0", "#27ae60")
        l2c_open_count = 0
        if 'Pipeline' in opps_filtered.columns and 'Status' in opps_filtered.columns:
            l2c_open_count = len(opps_filtered[(opps_filtered['Pipeline'] == 'L2C - Education') & (opps_filtered['Status'] == 'open')])
        okr_scorecard("L2C - Education (Open)", f"{l2c_open_count:,}", "#9b59b6")
        
        st.markdown("---")
        
        # Owner + Status
        st.subheader("👤 Owner & Status Analysis")
        col_chart1, col_chart2 = st.columns(2)
        with col_chart1:
            if 'Opportunity Owner' in opps_filtered.columns and 'Status' in opps_filtered.columns:
                owner_counts = opps_filtered.groupby('Opportunity Owner').size().reset_index(name='Total').sort_values('Total', ascending=False)
                top_9 = owner_counts.head(9)['Opportunity Owner'].tolist()
                opps_filtered = opps_filtered.copy()
                opps_filtered['Owner Category'] = opps_filtered['Opportunity Owner'].apply(lambda x: x if x in top_9 else 'Others')
                owner_status = opps_filtered.groupby(['Owner Category', 'Status']).size().reset_index(name='Count')
                owner_order = opps_filtered.groupby('Owner Category').size().sort_values(ascending=False).index.tolist()
                fig = px.bar(owner_status, x='Count', y='Owner Category', color='Status', title="Opportunity Owner (Top 9 + Others)", orientation='h', category_orders={'Owner Category': owner_order}, barmode='stack')
                fig.update_layout(height=400, yaxis_tickangle=0, paper_bgcolor=chart_bg, plot_bgcolor=chart_bg, font=dict(family="Helvetica", color=text_color))
                st.plotly_chart(fig, use_container_width=True)
        
        with col_chart2:
            if 'Status' in opps_filtered.columns:
                status_counts = opps_filtered.groupby('Status').size().reset_index(name='Count')
                fig = px.pie(status_counts, values='Count', names='Status', title="Status Distribution", hole=0.4)
                fig.update_traces(textposition='inside', textinfo='percent+label').update_layout(height=400, paper_bgcolor=chart_bg, font=dict(family="Helvetica", color=text_color))
                st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        
        # Funnel
        st.subheader("🔻 Pipeline Funnel - L2C Education")
        if 'Pipeline' in opps_filtered.columns and 'Status' in opps_filtered.columns:
            l2c_df = opps_filtered[opps_filtered['Pipeline'] == 'L2C - Education'].copy()
            if not l2c_df.empty:
                funnel_data = []
                if 'Stage' in l2c_df.columns:
                    for stage in STAGE_ORDER[:-1]:
                        stage_df = l2c_df[(l2c_df['Stage'] == stage) & (l2c_df['Status'] == 'open')]
                        if len(stage_df) > 0:
                            funnel_data.append({'Stage': stage, 'Count': len(stage_df), 'Value': stage_df['Opportunity Value'].sum() if 'Opportunity Value' in l2c_df.columns else 0})
                    won_df = l2c_df[l2c_df['Status'] == 'won']
                    if len(won_df) > 0:
                        funnel_data.append({'Stage': 'Won', 'Count': len(won_df), 'Value': won_df['Opportunity Value'].sum() if 'Opportunity Value' in l2c_df.columns else 0})
                if funnel_data:
                    funnel_df = pd.DataFrame(funnel_data)
                    fig = go.Figure(go.Funnel(y=funnel_df['Stage'], x=funnel_df['Count'], textinfo="value+percent initial",
                        hovertemplate='<b>%{y}</b><br>Count: %{x}<br>Value: $%{customdata:,.0f}<extra></extra>', customdata=funnel_df['Value']))
                    fig.update_layout(title="L2C Education Pipeline Funnel", height=500, paper_bgcolor=chart_bg, font=dict(family="Helvetica", color=text_color))
                    st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        
        # Lead Source
        st.subheader("📊 Lead Source Analysis (Top 10 + Others)")
        if 'Lead Source' in opps_filtered.columns and 'Status' in opps_filtered.columns:
            lead_source_counts = opps_filtered.groupby('Lead Source').size().reset_index(name='Total').sort_values('Total', ascending=False)
            top_10 = lead_source_counts.head(10)['Lead Source'].tolist()
            opps_filtered = opps_filtered.copy()
            opps_filtered['Lead Source Category'] = opps_filtered['Lead Source'].apply(lambda x: x if x in top_10 else 'Others')
            lead_status = opps_filtered.groupby(['Lead Source Category', 'Status']).size().reset_index(name='Count')
            lead_cat_order = opps_filtered.groupby('Lead Source Category').size().sort_values(ascending=False).index.tolist()
            fig = px.bar(lead_status, x='Count', y='Lead Source Category', color='Status', title="Lead Source (Top 10 + Others)", orientation='h', category_orders={'Lead Source Category': lead_cat_order}, barmode='stack')
            fig.update_layout(height=500, yaxis_tickangle=0, paper_bgcolor=chart_bg, plot_bgcolor=chart_bg, font=dict(family="Helvetica", color=text_color))
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        
        # Bubble Chart
        st.subheader("🔵 Opportunity Pipeline Bubble Chart")
        if 'Status' in opps_filtered.columns and 'Stage Percentage' in opps_filtered.columns:
            open_opps = opps_filtered[opps_filtered['Status'] == 'open'].copy()
            if not open_opps.empty and 'Lead Source' in open_opps.columns:
                bubble_data = open_opps.groupby('Lead Source').agg({'Opportunity Name': 'count', 'Opportunity Value': 'sum', 'Stage Percentage': 'mean'}).reset_index()
                bubble_data.columns = ['Lead Source', 'Count', 'Value', 'Avg Stage %']
                if not bubble_data.empty:
                    fig = px.scatter(bubble_data, x='Avg Stage %', y='Value', size='Count', color='Lead Source', title="Lead Source Performance (Open Opportunities)", hover_data={'Count': ':.0f'}, size_max=40)
                    fig.update_layout(height=500, paper_bgcolor=chart_bg, plot_bgcolor=chart_bg, font=dict(family="Helvetica", color=text_color)).update_xaxes(tickformat='.0f')
                    st.plotly_chart(fig, use_container_width=True)
                    st.caption("X-axis: Stage Progress (%), Bubble size: Opportunity Count")

# === TAB 5: CONTACT ANALYSIS ===
with tab5:
    st.header("👥 Contact Analysis")
    contacts = contact_data['contacts']
    if contacts.empty:
        st.warning("No contact data available")
    else:
        st.markdown("""<div class="filter-note"><b>ℹ️ Filter Applied:</b> This section uses Country/City filter below.</div>""", unsafe_allow_html=True)
        
        # Handle date_range for contacts - ensure we have date objects
        start_date_contact = date_range[0] if hasattr(date_range[0], 'date') else date_range[0]
        end_date_contact = date_range[1] if hasattr(date_range[1], 'date') else date_range[1]
        
        if 'Created Date' in contacts.columns:
            contacts_filtered = contacts[(contacts['Created Date'] >= start_date_contact) & (contacts['Created Date'] <= end_date_contact)].copy()
        else:
            contacts_filtered = contacts.copy()
        
        st.markdown(f"**Filtered Records:** {len(contacts_filtered)} contacts")
        
        # Location Filter
        st.subheader("🌍 Filter by Location")
        filter_type = st.radio("Filter Type:", ["Country", "City"], horizontal=True)
        selected_value = "All"
        
        if filter_type == "Country" and 'Country' in contacts_filtered.columns:
            countries = contacts_filtered['Country'].dropna().unique()
            countries = sorted([str(c) for c in countries if c])
            countries = ["All"] + countries
            selected_value = st.selectbox("Select Country", countries, key="country_select")
            if selected_value != "All": contacts_filtered = contacts_filtered[contacts_filtered['Country'] == selected_value]
        elif filter_type == "City" and 'City' in contacts_filtered.columns:
            cities = contacts_filtered['City'].dropna().unique()
            cities = sorted([str(c) for c in cities if c])
            cities = ["All"] + cities
            selected_value = st.selectbox("Select City", cities, key="city_select")
            if selected_value != "All": contacts_filtered = contacts_filtered[contacts_filtered['City'] == selected_value]
        
        st.markdown(f"**After Location Filter:** {len(contacts_filtered)} contacts")
        
        # Attribution
        st.subheader("📋 Attribution Source Comparison")
        if 'First Attribution Source' in contacts_filtered.columns and 'Latest Attribution Source' in contacts_filtered.columns:
            first_attr = contacts_filtered['First Attribution Source'].value_counts().reset_index()
            first_attr.columns = ['Attribution', 'First']
            latest_attr = contacts_filtered['Latest Attribution Source'].value_counts().reset_index()
            latest_attr.columns = ['Attribution', 'Latest']
            attr_comparison = pd.merge(first_attr, latest_attr, on='Attribution', how='outer').fillna(0)
            attr_comparison['First'] = attr_comparison['First'].astype(int)
            attr_comparison['Latest'] = attr_comparison['Latest'].astype(int)
            
            col_attr1, col_attr2 = st.columns(2)
            with col_attr1: st.dataframe(attr_comparison, use_container_width=True)
            with col_attr2:
                fig = go.Figure()
                fig.add_trace(go.Bar(x=attr_comparison['First'], y=attr_comparison['Attribution'], name='First Attribution', marker_color='#3498db', orientation='h'))
                fig.add_trace(go.Bar(x=attr_comparison['Latest'], y=attr_comparison['Attribution'], name='Latest Attribution', marker_color='#e74c3c', orientation='h'))
                fig.update_layout(title="First vs Latest Attribution", barmode='group', height=400, yaxis_tickangle=0, paper_bgcolor=chart_bg, font=dict(family="Helvetica", color=text_color))
                st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        
        # Lead Source + Source Tables
        col_table1, col_table2 = st.columns(2)
        with col_table1:
            st.subheader("📊 Lead Source Summary")
            if 'Lead Source' in contacts_filtered.columns:
                lead_source_summary = contacts_filtered.groupby('Lead Source').size().reset_index(name='Count').sort_values('Count', ascending=False)
                st.dataframe(lead_source_summary, use_container_width=True)
        with col_table2:
            st.subheader("📈 Source Summary")
            if 'Source' in contacts_filtered.columns:
                source_summary = contacts_filtered.groupby('Source').size().reset_index(name='Count').sort_values('Count', ascending=False)
                st.dataframe(source_summary, use_container_width=True)

# === TAB 6: CONSULTANT CAPACITY ===
with tab6:
    st.header("👨‍🏫 Consultant Capacity")
    consultants_today = consultant_data['consultants_today']
    consultants_weekly = consultant_data['consultants_weekly']
    
    col_chart1, col_chart2 = st.columns(2)
    with col_chart1:
        st.subheader("📅 Today's Capacity")
        if not consultants_today.empty:
            name_col = [c for c in consultants_today.columns if 'name' in c.lower() or 'consultant' in c.lower()]
            appt_col = [c for c in consultants_today.columns if 'appointment' in c.lower() or 'total' in c.lower()]
            if name_col and appt_col:
                fig = px.bar(consultants_today, x=appt_col[0], y=name_col[0], title="Today's Appointments", labels={name_col[0]: 'Consultant', appt_col[0]: 'Appointments'}, color=appt_col[0], color_continuous_scale='Blues', orientation='h')
                fig.update_layout(height=400, yaxis_tickangle=0, paper_bgcolor=chart_bg, plot_bgcolor=chart_bg, font=dict(family="Helvetica", color=text_color))
                st.plotly_chart(fig, use_container_width=True)
        else: st.info("No today's capacity data")
    
    with col_chart2:
        st.subheader("📆 Weekly Capacity")
        if not consultants_weekly.empty:
            name_col = [c for c in consultants_weekly.columns if 'name' in c.lower() or 'consultant' in c.lower()]
            appt_col = [c for c in consultants_weekly.columns if 'appointment' in c.lower() or 'total' in c.lower()]
            if name_col and appt_col:
                fig = px.bar(consultants_weekly, x=appt_col[0], y=name_col[0], title="Weekly Appointments", labels={name_col[0]: 'Consultant', appt_col[0]: 'Appointments'}, color=appt_col[0], color_continuous_scale='Greens', orientation='h')
                fig.update_layout(height=400, yaxis_tickangle=0, paper_bgcolor=chart_bg, plot_bgcolor=chart_bg, font=dict(family="Helvetica", color=text_color))
                st.plotly_chart(fig, use_container_width=True)
        else: st.info("No weekly capacity data")
    
    st.markdown("---")
    
    col_table1, col_table2 = st.columns(2)
    with col_table1:
        st.subheader("📋 Today's Details")
        if not consultants_today.empty: st.dataframe(consultants_today, use_container_width=True)
        else: st.info("No data")
    with col_table2:
        st.subheader("📋 Weekly Details")
        if not consultants_weekly.empty: st.dataframe(consultants_weekly, use_container_width=True)
        else: st.info("No data")

#streamlit run "c:\Users\Hafsa Saleh\IdeaProjects\Themigration\ghl integration\mktdashboard.py"
