import streamlit as st


def page_frame(title, subtitle=None, show_filters=True):
    """Standard page frame with title, subtitle, divider, and optional sidebar filters."""
    st.title(title)
    if subtitle:
        st.caption(subtitle)
    st.divider()
    if show_filters:
        with st.sidebar:
            st.subheader("Filters")
