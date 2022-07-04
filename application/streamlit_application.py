import pathlib

import streamlit as st
from jinja2 import Environment, FileSystemLoader
import uuid
import os
import collections
import sys
import pathlib
sys.path.append(str(pathlib.Path(__file__).parent.parent.parent.resolve()))
from code_backup.web_app import utils
from code_backup.web_app.user_info import info

# Set page title and favicon.
MAGE_EMOJI_URL = ""
st.set_page_config(
    page_title="HedgeBlocked", page_icon=MAGE_EMOJI_URL,layout="wide"
)

user_info = info(0)
print("user_info.user_id:",user_info.user_id)



def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == st.secrets["password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store password
        else:
            st.session_state["password_correct"] = False
    def show_content():
        # Display header.
        st.markdown("<br>", unsafe_allow_html=True)
        # st.image(MAGE_EMOJI_URL, width=80)
        st.write(
            """
            # Use Blockchain to create unchangable verified trade record.
            HedgeBlock is to use Blockchain to create unchangable verified record for fund managers:  The record is encrypted and record needs 2 factor verification between uploaded and viewer.
    
            """
        )
        st.markdown("<br>", unsafe_allow_html=True)
        st.write(
            """
            # HedgeBlock blcokchained features including:
            1. Detail trade log 
            2. Portfolio analyst
            3. Portfolio visualization
            4. Letter to investor
            ---
            """
        )

    if "password_correct" not in st.session_state:
        # First run, show input for password.
        st.text_input(
            "One-time-Passcode is sent to the trader, please contact the trader", type="password", on_change=password_entered, key="password"
        )
        st.button("Resend Passcode")
        show_content()
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        st.text_input(
            "One-time-Passcode is sent to the trader, please contact the trader", type="password", on_change=password_entered, key="password"
        )
        st.error("😕 Password incorrect")
        st.button("Resend Passcode")
        show_content()
        return False
    else:
        # Password correct.
        return True

if check_password():
    st.write("Your session will be automatically logout in 1 hour.  Please request another passcode after one hour")

    page_dict = collections.defaultdict(dict)
    path = str(pathlib.Path(__file__).parent.parent.resolve()) + f"/frontend/web_app"
    page_dirs = [
        f for f in os.scandir(path) if f.is_dir() and f.name != "example" and f.name != "__pycache__"
    ]
    page_dirs = sorted(page_dirs, key=lambda e: e.name)
    for page_dir in page_dirs:
        page_dict[page_dir.name] = page_dir.path

    print(page_dict)

    # Show selectors for task and framework in sidebar (based on template_dict). These
    # selectors determine which template (from template_dict) is used (and also which
    # template-specific sidebar components are shown below).
    with st.sidebar:
        st.info(
            "This Web-app is to log the trade record in Blockchain using AWS [AWS Blockchain](https://aws.amazon.com/tw/qldb/)"
        )
        st.write("## HedgeBlocked")
        task = st.selectbox(
            "Select fund information", list(page_dict.keys())
        )
        selected_page_dir = page_dict[task]


    # Show template-specific sidebar components (based on sidebar.py in the template dir).
    selected_page_sidebar = utils.import_from_file(
        "selected_page_sidebar", os.path.join(selected_page_dir, "sidebar.py")
    )
    selected_page = utils.import_from_file(
        "selected_page", os.path.join(selected_page_dir, "page.py")
    )


    inputs = selected_page_sidebar.show()
    selected_page.show(inputs, user_info)

    if os.getenv("TRACKING_NAME"):
        f"![](https://jrieke.goatcounter.com/count?p={os.getenv('TRACKING_NAME')})"
