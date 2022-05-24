import pathlib

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
from frontend.web_app import utils


def show(inputs, user_info):
    st.write("User ID:",user_info.user_id)

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

    stats_table = user_info.stats_data_dir+"all_file_return.csv"
    print("stats_table:" + stats_table)
    stats_df = pd.read_csv(stats_table)
    selection = aggrid_interactive_table(df=stats_df)
    if selection:
        if len(selection["selected_rows"])>0:
            selected_dict = selection["selected_rows"][0]
            backtest_spec = selected_dict["Backtest Spec"]
            st.write(backtest_spec)
            chart_data_path = user_info.run_file_dir +backtest_spec+".csv"
            chart_data = pd.read_csv(chart_data_path)
            st.line_chart(chart_data.set_index("date")["NetLiquidation"])
            df = pd.DataFrame.from_dict(selected_dict, orient='index').rename(columns={0: 'Stats'}).astype(str)

            print(df)
            st.table(df)