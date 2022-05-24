# -*- coding: utf-8 -*-
# Sample Python code for youtube.channels.list
# See instructions for running these code samples locally:
# https://developers.google.com/explorer-help/guides/code_samples#python <--- Please refer to this website to know more about youtube API service

#!/usr/bin/python3.7
import os
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import googleapiclient.errors
import google_auth_oauthlib.flow
import googleapiclient.discovery
import time
from time import gmtime, strftime

current_dir=os.path.dirname(os.path.realpath(__file__))
credential_status_and_youtube_health_log_dir=os.path.join(current_dir,'log')
asset_dir=os.path.join(current_dir,'asset')
client_secrets_file = os.path.join(asset_dir,"client_secret_373495315509-7tbhbus6l73trf5jdonuodjv0bapmlig.apps.googleusercontent.com.json")
CREDENTIALS_PICKLE_FILE_path= os.path.join(asset_dir,"CREDENTIALS_PICKLE_FILE")
api_service_name = "youtube"
api_version = "v3"
scopes = ["https://www.googleapis.com/auth/youtube.readonly"] ## To define the scope of the API service. Readonly only allows access for channel information, you cannot modify anything on youtube server


class youtube_health_engine:

    def __init__(self,stream_id,stream_key,log_dir):
        self.id=stream_id
        self.key=stream_key
        self.log_dir=os.path.join(log_dir,'Credential_status_and_youtube_health_stream_log_of_stream'+str(self.id)+'.txt')

    # To obtain a youtube health json object from youtube API, where the json file contains all the currently livestreaming videos of the authenticated user
    def get_youtube_health_json(self):
        # Disable OAuthlib's HTTPS verification when running locally.
        # *DO NOT* leave this option enabled in production.
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
        # Get credentials and create an API client
        flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
            client_secrets_file, scopes)
        youtube = self.get_authenticated_service()

        ## youtube.liveStreams().list() method list out all the information of the youtube channel interested
        request = youtube.liveStreams().list(part='status,cdn', mine=True) # cdn contains stream_key information, status contains health and stream status, mine is to obtain the youtube status of the channel of this credential file user
        
        response = request.execute()
        return response # in json format

    # To obtain stream status and health status for one stream key
    def get_youtube_stream_health(self):
        response=self.get_youtube_health_json()
        stream_status=''
        health_status=''
        for video in response['items']:
            if video['cdn']['ingestionInfo']['streamName']==self.key: ## video['cdn']['ingestionInfo']['streamName'] is the stream key
                stream_status=video['status']['streamStatus']
                health_status=video['status']['healthStatus']['status'] ## obtain the status of that stream key
            else:
                pass
        print('The stream status is '+stream_status)
        return stream_status,health_status
            

    # To check if the token stored in CREDENTIALS_PICKLE_FILE_path expires. If it expires then obtain a refreshed one. If there is no token at all then create a new Pickle file, to get authenticated and to store that token into the pickle file
    # This function returns an authenticated object which can be used for enquiry of youtube channel health
    def get_authenticated_service(self):
        if os.path.exists(CREDENTIALS_PICKLE_FILE_path):
            with open(CREDENTIALS_PICKLE_FILE_path, 'rb') as f:
                credentials = pickle.load(f)
                if credentials is None or credentials.expired:
                    if credentials is None:
                        print('Credential does not exist. Now retrieve tokens.')
                        flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(client_secrets_file, scopes)
                        credentials = flow.run_console()
                        with open(CREDENTIALS_PICKLE_FILE_path, 'wb') as f:
                            pickle.dump(credentials, f)
                    else:
                        f = open(self.log_dir,'a')
                        f.write('Credential expires! This log is registered at ' + str(strftime("%a, %d %b %Y %H:%M:%S")) + '\n')
                        f.close()
                        print('Credential expires. Now refreshing...')
                        credentials.refresh(Request())
                        with open(CREDENTIALS_PICKLE_FILE_path, 'wb') as f:
                            pickle.dump(credentials, f)
                            print('Finish writing the renewed credential into pickle file.')
                else:
                    f = open(self.log_dir,'a')
                    f.write('Credential is good! This log is registered at ' + str(strftime("%a, %d %b %Y %H:%M:%S")) + '\n')
                    f.close()
                
        else:
            print('Pickle file does not exist. Now creating new one and retreive token.')
            flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(client_secrets_file, scopes)
            credentials = flow.run_console()
            with open(CREDENTIALS_PICKLE_FILE_path, 'wb') as f:
                pickle.dump(credentials, f)
        return googleapiclient.discovery.build(
            api_service_name, api_version, credentials=credentials)

def main():
    engine=youtube_health_engine(stream_id=0,stream_key='',log_dir='')
    engine.get_youtube_stream_health()

if __name__ == "__main__":
    main()



        




