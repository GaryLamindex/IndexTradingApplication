import base64
import pathlib

import pandas as pd
import streamlit as st
from PIL import Image
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
from pythonProject.frontend.web_app import utils
from streamlit_option_menu import option_menu
from streamlit_text_rating.st_text_rater import st_text_rater



def show(inputs, user_info):

    def show_container(title, pdf_path, key):
        feature_image = Image.open(r'C:\Users\lam\Documents\GitHub\test_blog_data\reports\feature_image1.jpg')
        with st.container():
            image_col, text_col = st.columns((1, 3))
            with image_col:
                st.image(feature_image, caption='Image by Pixabay')
            with text_col:
                st.markdown(""" <style> .font {
                font-size:14px ; font-family: 'Black'; color: #FFFFF;} 
                </style> """, unsafe_allow_html=True)
                st.markdown(f'<p class="font">{title}</p>', unsafe_allow_html=True)

        if st.button('Read/Download PDF', key=str(key)):
            show_pdf(pdf_path)

        st.write('---')

    def show_reports():
        show_container("Quarterly Performance",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\blockchain shopify ideation.pdf', 0)
        show_container("One Pager",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 1)
        show_container("Annual Report",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\blockchain shopify ideation.pdf', 2)
        show_container("Fact Sheets",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\blockchain shopify ideation.pdf', 3)
        pass

    def show_prospectus():
        show_container("Prospectus",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\blockchain shopify ideation.pdf', 0)
        show_container("Investment case",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 1)
        pass

    def show_research():
        show_container("White paper",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\blockchain shopify ideation.pdf', 0)
        show_container("Investment case",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 1)
        pass

    def show_fund_holdings():
        show_container("2022 Q1 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\blockchain shopify ideation.pdf', 0)
        show_container("2021 Q4 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 1)
        show_container("2021 Q3 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 2)
        show_container("2021 Q2 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 3)
        show_container("2021 Q1 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 4)
        show_container("2020 Q4 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 5)
        show_container("2020 Q3 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 6)
        show_container("2020 Q2 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 7)
        show_container("2020 Q1 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 8)
        show_container("2019 Q4 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 9)
        show_container("2019 Q3 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 10)
        show_container("2019 Q2 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 11)
        show_container("2019 Q1 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 12)
        show_container("2018 Q4 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 13)
        show_container("2018 Q3 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 14)
        show_container("2018 Q2 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 15)
        show_container("2018 Q1 Holdings",
                       'C:\\Users\\lam\\Documents\\GitHub\\test_blog_data\\reports\\test white paper.pdf', 16)
        pass

    def show_pdf(file_path):
        with open(file_path, "rb") as f:
            base64_pdf = base64.b64encode(f.read()).decode('utf-8')
        pdf_display = f'<iframe src="data:application/pdf;base64,{base64_pdf}" width=100% height="800" type="application/pdf"></iframe>'
        st.markdown(pdf_display, unsafe_allow_html=True)

    st.write("User ID:",user_info.user_id)

    topic = option_menu(None, ["Reports", "Prospectus", "Research", "Fund Holdings"],
                        icons=['book', 'book', 'book', 'book', 'book'],
                        menu_icon="list", default_index=0,
                        styles={
                            "container": {"padding": "0!important", "background-color": "#fafafa"},
                            "icon": {"color": "orange", "font-size": "14px"},
                            "nav-link": {"font-size": "14px", "text-align": "left", "margin": "0px",
                                         "--hover-color": "#eee"},
                            "nav-link-selected": {"background-color": "#080000"},
                        }, orientation='horizontal'
                        )
    st.write('')


    if topic == 'Reports':
        show_reports()
    if topic == 'Prospectus':
        show_prospectus()
    if topic == 'Research':
        show_research()
    if topic == 'Fund Holdings':
        show_fund_holdings()


