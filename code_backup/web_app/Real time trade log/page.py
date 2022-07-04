import pathlib

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
from pythonProject.frontend.web_app import utils


def show(inputs, user_info):
    st.write("User ID:",user_info.user_id)
    st.write("real time trade log")
    st.write(inputs)

    def aggrid_interactive_table(df: pd.DataFrame):
        """Creates an st-aggrid interactive table based on a dataframe.
        Args:
            df (pd.DataFrame]): Source dataframe
        Returns:
            dict: The selected row
        """
        options = GridOptionsBuilder.from_dataframe(
            df, enableRowGroup=True, enableValue=True, enablePivot=True
        )

        options.configure_side_bar()

        options.configure_selection("single")
        selection = AgGrid(
            df,
            enable_enterprise_modules=True,
            gridOptions=options.build(),
            theme="light",
            update_mode=GridUpdateMode.MODEL_CHANGED,
            allow_unsafe_jscode=True,
        )

        return selection

    trade_log_table = user_info.run_file_dir+"0.05_rebalance_margin_0.005_max_drawdown_ratio_5.0_purchase_exliq_.csv"
    print("user_info.run_file_dir:" + user_info.run_file_dir)
    print("trade_log_table:"+trade_log_table)
    log_df = pd.read_csv(trade_log_table)
    chart_data = log_df["NetLiquidation"]

    st.line_chart(chart_data)
    selection = aggrid_interactive_table(df=log_df)

    if selection:
        st.write("You selected:")
        st.json(selection["selected_rows"])
