import os
from time import gmtime, strftime
import time
import subprocess
import sys

class youtube_streaming_engine:
    
    def __init__(self,stream_id,stream_key,log_dir): # stream_id should be a positive integer, with zero included.
        self.id = stream_id
        self.key=stream_key
        self.log_dir=log_dir
        self.current_path=os.path.dirname(os.path.realpath(__file__))

    
    # Check if id_directory exists. For example, to check if there is a directory called stream_0 in under dir .../streaming_engine/asset/
    def check_if_id_directory_exists(self):
        dir=os.path.join(self.current_path,'asset','stream_'+str(self.id))
        if os.path.exists(dir):
            print('Directory of stream_'+str(self.id)+' exists!'+'\n'+'Its directory is '+dir)
        else:
            os.makedirs(dir)
            print('Directory of stream_'+str(self.id)+' does not exist!'+'\n'+'A new directory is created!'+'\n'+'Its directory is '+dir)

    # call ffmpeg via subprocess and monitors its health
    def youtube_streaming_continuously(self):

        # subprocess.DEVNULL is to direct the standard output of this process to a null file(to disable showing any standard output and error)
        ffmpeg = subprocess.Popen(['../../../../algotrade_venv/Scripts/python', 'ffmpeg_engine.py',str(self.id),self.key,self.log_dir], shell=False,cwd=self.current_path,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) 
        
        while True:
            if ffmpeg.poll() != None:
                ffmpeg.kill()
                time.sleep(8)
                self.youtube_streaming_continuously()
            else:
                continue




def main():
    engine=youtube_streaming_engine(stream_id=sys.argv[1],stream_key=sys.argv[2],log_dir=sys.argv[3])
    engine.youtube_streaming_continuously()


if __name__ == "__main__":
    main()