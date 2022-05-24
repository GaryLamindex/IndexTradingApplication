# -*- coding: utf-8 -*-
# Sample Python code for youtube.channels.list
# See instructions for running these code samples locally:
# https://developers.google.com/explorer-help/guides/code_samples#python
#!/usr/bin/python3.7
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

def main():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    # Get credentials and create an API client
    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
        client_secrets_file, scopes)
    youtube = get_authenticated_service()
    request = youtube.channels().list(
        part="contentDetails",
        mine=True
    )
    response = request.execute()
    print(response)

def get_authenticated_service():
    if os.path.exists("CREDENTIALS_PICKLE_FILE"):
        with open("CREDENTIALS_PICKLE_FILE", 'rb') as f:
            credentials = pickle.load(f)
            if credentials is None or credentials.expired:
                if credentials is None:
                    print('Credential does not exist. Now retrieve tokens.')
                    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(client_secrets_file, scopes)
                    credentials = flow.run_console()
                    with open("CREDENTIALS_PICKLE_FILE", 'wb') as f:
                        pickle.dump(credentials, f)
                else:
                    print('Credential is expired. Now refreshing...')
                    credentials=credentials.refresh(Request())
                    with open("CREDENTIALS_PICKLE_FILE", 'wb') as f:
                        pickle.dump(credentials, f)
                        print('Finish writing the renewed credential into pickle file.')
            else:
                print('Credential status is good.')
                print(credentials.expired)
                print(credentials.valid)
            
    else:
        print('Pickle file does not exist. Now creating new one and retreive token.')
        flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(client_secrets_file, scopes)
        credentials = flow.run_console()
        with open("CREDENTIALS_PICKLE_FILE", 'wb') as f:
            pickle.dump(credentials, f)
    return googleapiclient.discovery.build(
        api_service_name, api_version, credentials=credentials)

if __name__ == "__main__":
    main()

# if __name__ == "__main__":
#     main()
    # response = request.execute()
    # print(response['items'][0]['status']['healthStatus']['status'])



#   while True:
#                 #response = request.execute()
#                 if ffmpeg.poll() != None:
#                     if response['items'][0]['status']['healthStatus']['status'] == 'noData':
#                         time.sleep(25)
#                         response = request.execute()
#                         if response['items'][0]['status']['healthStatus']['status'] == 'noData':
#                             ffmpeg.kill()
#                             time.sleep(8)
#                             main()
#                         else:
#                             continue
#                     else:
#                         writer_object = csv.writer(f_object)
#                         string = 'Output message printed at datetime: ' + str(strftime(
#                             "%a, %d %b %Y %I:%M:%S")) + '.' + '\n' + 'The first column next to this message cell is output of ffmpeg streaming process, and the second message is the error. FFmpeg is terminated. Restart after 8 seconds.'
#                         list = [string, str(out), str(err)]
#                         writer_object.writerow(list)
#                         f_object.close()
#                         ffmpeg.kill()
#                         time.sleep(8)
#                         main()
#                 else:
#                     writer_object = csv.writer(f_object)
#                     string = 'Output message printed at datetime: ' + str(strftime(
#                         "%a, %d %b %Y %I:%M:%S")) + '.' + '\n' + 'The first column next to this message cell is output of ffmpeg streaming process, and the second message is the error. FFmpeg is running normally.'
#                     print(response)
#                     list = [string, str(out), str(err)]
#                     writer_object.writerow(list)
#                     f_object.close()
#                     time.sleep(20)


        




