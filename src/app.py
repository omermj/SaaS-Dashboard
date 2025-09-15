import streamlit as st
from ui.layout import page_frame

st.set_page_config(page_title="SaaS Dashboard", layout="wide")
page_frame(
    title="SaaS Dashboard",
    subtitle="Monitor your SaaS metrics in real-time",
    show_filters=False,
)
st.write(
    "Welcome to the SaaS Dashboard! Use the sidebar to navigate through different metrics and reports."
)
