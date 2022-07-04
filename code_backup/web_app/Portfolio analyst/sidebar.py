import streamlit as st


# Define possible models in a dict.
# Format of the dict:
# option 1: model -> code
# option 2 – if model has multiple variants: model -> model variant -> code

SIDEBAR_SELECTION = {
    "Since Inception": "since_inception",
    "YTD Yr Performacne": "ytd_performance",
    "1 Yr Performacne": "1_yr_performance",
    "3 Yr Performacne": "3_yr_performance",
    "5 Yr Performacne": "5_yr_performance"
    }

def show():
    """Shows the sidebar components for the template and returns user inputs as dict."""

    inputs = {}
    with st.sidebar:
        st.write("## Please select time-frame for NAV Graph display")
        info_selection = st.selectbox("Please select time-frame", list(SIDEBAR_SELECTION.keys()))

        inputs["info_func"] = SIDEBAR_SELECTION[info_selection]
        st.markdown(
            "<sup>Default: Max time-period as default</sup>",
            unsafe_allow_html=True,
        )

        st.write(
            """
        Statistic included: `[NAV, Return, Sharpe ratio, Sortino ratio, Max drawdown]`.  All the statistical data is calculated by HedgeBlocked from raw trading history provided by trader
        - `NAV` has graph of historical data 
        - `Return` has table of ytd, 1yr, 3yr, 5yr historical return
        - `Sharpe ratio` has table of ytd, 1yr, 3yr, 5yr Sharpe ratio 
        - `Sortino ratio` has table of Sortino ratio within a period of ytd, 1yr, 3yr, 5yr 
        - `Max drawdown` has table of max drawdown within a period of  ytd, 1yr, 3yr, 5yr         """
        )

    return inputs


if __name__ == "__main__":
    show()
