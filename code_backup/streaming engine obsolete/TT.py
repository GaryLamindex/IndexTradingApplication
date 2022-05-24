import os
import pickle
import google_auth_oauthlib.flow
import googleapiclient.discovery
from google.auth.transport.requests import Request
import httplib2

import os
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import googleapiclient.errors
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]
client_secrets_file = "client_secret_373495315509-7tbhbus6l73trf5jdonuodjv0bapmlig.apps.googleusercontent.com.json"
api_service_name = "youtube"
api_version = "v3"

with open("CREDENTIALS_PICKLE_FILE", 'rb') as f:
    credentials = pickle.load(f)
    credentials=credentials.refresh(Request())
    with open("CREDENTIALS_PICKLE_FILE", 'wb') as f:
        pickle.dump(credentials, f)
