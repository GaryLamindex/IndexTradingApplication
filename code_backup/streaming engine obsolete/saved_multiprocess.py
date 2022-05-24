import sys
import pathlib
import subprocess
import os
import time
import youtube_health_engine 
from time import gmtime, strftime


class multiprocess:
                        ##Format of kwargs is stream_(id) =['id','youtube_stream_key'], which is a list
    def __init__(self): ##Example of kwargs input: stream_1=[1,'u7m4-y3y6-xs1z-saha-1ke8'],stream_2=[2,'mspa-j0rh-zkqx-m7g7-14jv']...  kwargs takes in format of a list, which contains stream id and its youtube stream key
        self.dir_path = os.path.dirname(os.path.realpath(__file__))
        self.id=sys.argv[1] 
        self.key=sys.argv[2]

    def multiprocess(self):
        times_of_restart=0
        youtube_health=youtube_health_engine.youtube_health_engine(self.id,self.key) ## initiating class youtube_health_engine in youtube_health_engine.py in this directory
        youtube=subprocess.Popen(['../../../../algotrade_venv/Scripts/python', 'youtube_streaming_engine.py',self.id,self.key], shell=False,cwd=self.dir_path,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        img=subprocess.Popen(['../../../../algotrade_venv/Scripts/python', 'img_generation_engine.py',self.id], shell=False,cwd=self.dir_path,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print('FFmpeg is initiated! Wait for 60 seconds and start checking its health status')
        time.sleep(60)
        while True:
            stream_status,health_status=youtube_health.get_youtube_stream_health()
            print('Time Now is '+ str(strftime("%a, %d %b %Y %H:%M:%S")))
            if health_status=='noData':
                print('Youtube server receives no data! Now wait for 60 seconds and recheck its health status.')
                time.sleep(60)
                stream_status,health_status=youtube_health.get_youtube_stream_health()
                if health_status=='noData':
                    times_of_restart += 1
                    print('Youtube server still receives no data after 60 seconds, restarting ffmpeg...')
                    print('Number of times of restart is '+str(times_of_restart))
                    youtube.kill()
                    print('FFmpeg subprocess is killed, wait for 60 seconds to restart')
                    time.sleep(60)
                    youtube=subprocess.Popen(['../../../../algotrade_venv/Scripts/python', 'youtube_streaming_engine.py',self.id], shell=False,cwd=self.dir_path,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    print('FFmpeg is restarted, wait for 40 seconds to check for its health.')
                    time.sleep(40)
                else:
                    print('Youtube server receives data now. Its health status now is '+ health_status)
            if img.poll() != None:
                print('Image generation engine has died, restarting after 8 seconds.')
                img.kill()
                time.sleep(8)
                img=subprocess.Popen(['../../../../algotrade_venv/Scripts/python', 'img_generation_engine.py',self.id], shell=False,cwd=self.dir_path,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                if health_status != 'noData':
                    print('Health status of youtube for stream id '+str(self.id)+' is '+health_status)
                else:
                    pass
                print('The status of youtube_'+self.id+' is ' + str(youtube.poll()))
                print('The status of img_'+self.id+' is '+str(img.poll()))
                time.sleep(10)

def main():
    engine=multiprocess()
    engine.multiprocess()

if __name__ == "__main__":
    main()
