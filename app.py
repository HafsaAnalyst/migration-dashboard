"""
THE MIGRATION - PRODUCTION MARKETING INTELLIGENCE DASHBOARD
Single-file consolidated deployment for Streamlit Community Cloud.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import asyncio
from datetime import datetime, date, timedelta
import json
import traceback
import os
import sys

# Import Async Clients
from ghl_async_client import GHLAsyncClient
from meta_async_client import MetaAsyncClient, fetch_meta_data
from ga4_async_client import fetch_ga4_data
from gsc_async_client import fetch_gsc_data
import statsmodels.api as sm
import pytz

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="The Migration | Marketing Performance Dashboard",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- AUTHENTICATION ---
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

def login_gate():
    if not st.session_state.authenticated:
        # Try to get credentials from secrets, or use defaults for local testing
        try:
            auth_user = st.secrets["auth"]["username"]
            auth_pass = st.secrets["auth"]["password"]
        except:
            # Fallback for local testing
            auth_user = "themigration"
            auth_pass = "1900clients"
            
        st.markdown("<div style='text-align: center; padding-top: 100px;'>", unsafe_allow_html=True)
        st.title("🔐 Marketing Performance Login")
        user = st.text_input("Username")
        pw = st.text_input("Password", type="password")
        if st.button("Login"):
            if user == auth_user and pw == auth_pass:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Invalid credentials")
        st.markdown("</div>", unsafe_allow_html=True)
        st.stop()

login_gate()

# --- THEME MANAGEMENT ---
if "theme_choice" not in st.session_state:
    st.session_state.theme_choice = "Dark"

with st.sidebar:
    st.markdown("""
        <div style="padding: 0.5rem 0 1.5rem; border-bottom: 1px solid #2d2f31; margin-bottom: 1.5rem;">
            <span style="color: white; font-weight: bold; font-size: 1.5rem;">The Migration</span>
        </div>
    """, unsafe_allow_html=True)
    
    # Global Date Filter
    # Default to Nov 1st 2025 as per project history
    default_start = date(2025, 11, 1)
    default_end = date.today()
    date_range = st.date_input("Select Range", [default_start, default_end])
    
    st.markdown("<div style='margin-top: auto; padding-top: 1rem; border-top: 1px solid #2d2f31;'></div>", unsafe_allow_html=True)
    choice = st.radio("Appearance", ["Dark", "Light"], index=0 if st.session_state.theme_choice == "Dark" else 1)
    if choice != st.session_state.theme_choice:
        st.session_state.theme_choice = choice
        st.rerun()

# --- THEME VARIABLES ---
if st.session_state.theme_choice == "Dark":
    bg_color, surface_color, text_color = "#0f172a", "#1e293b", "#f8fafc"
    secondary_text, accent, border_color = "#94a3b8", "#2dd4bf", "#334155"
    table_bg, chart_bg, plotly_template = "#000000", "#1e293b", "plotly_dark"
    card_shadow = "0 10px 15px -3px rgba(0,0,0,0.3)"
    chart_text_color = "#f8fafc"
else:
    bg_color, surface_color, text_color = "#f1f5f9", "#ffffff", "#000000"
    secondary_text, accent, border_color = "#475569", "#0d9488", "#cbd5e1"
    table_bg, chart_bg, plotly_template = "#ffffff", "#ffffff", "plotly_white"
    card_shadow = "0 4px 6px -1px rgba(0, 0, 0, 0.1)"
    chart_text_color = "#000000"
