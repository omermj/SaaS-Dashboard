import math
import pandas as pd


def fmt_money(x):
    return f"${x:,.0f}"


def fmt_pct(x):
    return f"{x:.1%}"


def fmt_months(x):
    return "∞" if x >= 9999 else f"{x:.0f} mo"


def fmt_multiple(x):
    return "∞" if x == float("inf") else f"{x:.1f}x"


def fmt_margin(x):
    return "N/A" if pd.isna(x) else fmt_pct(x)
