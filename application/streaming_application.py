import sys
import pathlib
import subprocess
import os
import pathlib
import time
from time import gmtime, strftime


class streaming_application:

    stream_dicts = {} # To store kwargs retrieved from main_application
    credential_status_and_youtube_health_log_dir ='' # To store the log directory for credential_status of youtube API

    def __init__(self, dict):

        self.par_path = os.path.dirname(pathlib.Path(__file__).parent) # Obtain parent directory of this script
        self.engine_dir = os.path.join(self.par_path, 'engine', 'streaming_engine') # Obtain engine directory which stores streaming_engine.py
        self.kwargs = eval(dict) # Turn the dictionary string back to dictionary

    # To sort kwargs into a dictionary
    def grab_kwargs(self):
        for stream_key in self.kwargs:
            stream_dict = {"user_id": self.kwargs[stream_key][0], "stream_key": self.kwargs[stream_key][1],
                           "log_dir": self.kwargs[stream_key][2]}
            self.stream_dicts[str(stream_key)]=stream_dict
        self.credential_status_and_youtube_health_log_dir=self.stream_dicts[next(iter(self.stream_dicts))]['log_dir'] # To store the log directory as self.credential_status_and_youtube_health_log_dir

    def streaming(self):

        # To call grab_kwargs function
        self.grab_kwargs() 

        # For each youtube stream key we create one subprocess via multiprocess.py
        for key in self.stream_dicts:

            # To write down the log time for credential status log
            dir = os.path.join(self.credential_status_and_youtube_health_log_dir,
                               'Credential_status_and_youtube_health_stream_log_of_stream' + str(self.stream_dicts[key]['user_id']) + '.txt')
            f = open(dir, 'w+')
            f.write('This output log starts at ' + str(strftime("%a, %d %b %Y %H:%M:%S")) + '\n')
            f.close()

            
            key = subprocess.Popen(['python', 'multiprocess_engine.py', str(self.stream_dicts[key]['user_id']), self.stream_dicts[key]['stream_key'],self.stream_dicts[key]['log_dir']],
                                           shell=False, cwd=self.engine_dir)


def main(dict):
    engine = streaming_application(dict)
    engine.streaming()


if __name__ == "__main__":
    main(sys.argv[1])