from datetime import datetime, date

import streamlit as st


def show():
    """Shows the sidebar components for the template and returns user inputs as dict."""

    inputs = {}

    with st.sidebar:
        st.write("## Block-chained trade log, powered by HedgeBlcoked")

        inputs["start_date"] = st.date_input(
            "Please select trade log start date", date(int(datetime.today().year), int(datetime.today().month), int(datetime.today().day)))
        st.markdown(
            "<sup>The minimum selectable date is 10 years from now</sup>",
            unsafe_allow_html=True,
        )

        inputs["end_date"] = st.date_input(
            "Please select trade log end date", date( datetime.today().year,  datetime.today().month,  datetime.today().day))
        st.markdown(
            "<sup>The minimum selectable date is 10 years from now</sup>",
            unsafe_allow_html=True,
        )
        if inputs["end_date"] < inputs["start_date"]:
            st.markdown(
                "<sup>Expected error `End date is larger than start date`</sup>",
                unsafe_allow_html=True,
            )
        st.write(
                """
            Return format: `[Trade, Action, Quantity, Datetime, Price]`
            - `Trade` has (ticker symbol, Exchange)
            - `Action` has (Bought, Sold)
            - `Quantity` has (Action quantity)
            - `Datetime` has (Date, time)
            - `Price` has (Avg price/order)
            """
        )

    return inputs


if __name__ == "__main__":
    show()
