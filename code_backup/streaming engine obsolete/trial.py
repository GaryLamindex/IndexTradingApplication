# YouTube Data API v3
# Pulling data for the brand account

import os
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

credentials = None


# youtube_data_token_brand.pickle stores the user's credentials from previously successful logins
if os.path.exists('youtube_data_token_brand.pickle'):
    print('Loading Credentials From File...')
    with open('youtube_data_token_brand.pickle', 'rb') as token:
        credentials = pickle.load(token)

# If there are no valid credentials available, then either refresh the token or log in.
if not credentials or not credentials.valid:
    if credentials and credentials.expired and credentials.refresh_token:
        print('Refreshing Access Token...')
        credentials.refresh(Request())
    else:
        print('Fetching New Tokens...')
        flow = InstalledAppFlow.from_client_secrets_file(
            'client_secrets_youtube_data_brand.json',
            scopes=[
                'https://www.googleapis.com/auth/youtube.readonly'
            ]
        )

        flow.run_local_server(port=8080, prompt='consent',
                              authorization_prompt_message='')
        credentials = flow.credentials

        # Save the credentials for the next run
        with open('youtube_data_token_brand.pickle', 'wb') as f:
            print('Saving Credentials for Future Use...')
            pickle.dump(credentials, f)

youtube = build('youtube', 'v3', credentials=credentials)

request = youtube.playlistItems().list(
        part="status, contentDetails", playlistId='UUlG34RnfYmCsNFgxmTmYjPA', maxResults=28
    )

response = request.execute()

def main():
    test=png()
    test.png()


        
if __name__ == "__main__":
    main()