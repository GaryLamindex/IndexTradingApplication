import sys
import pathlib
import subprocess
import os
import time
import youtube_health_engine 
from time import gmtime, strftime

class multiprocess:
                        
    def __init__(self): 
        self.dir_path = os.path.dirname(os.path.realpath(__file__))
        self.id=sys.argv[1] 
        self.key=sys.argv[2]
        self.log_dir=sys.argv[3] # sys.argv[3] takes the third argument of the commandline

    # To spawn one youtube_streaming_engine and one img_generation_engine subprocess for one stream key
    def multiprocess(self):


        youtube_health=youtube_health_engine.youtube_health_engine(stream_id=self.id,stream_key=self.key,log_dir=self.log_dir) ## initiating class youtube_health_engine in youtube_health_engine.py

        youtube=subprocess.Popen(['../../../../algotrade_venv/Scripts/python', 'youtube_streaming_engine.py',self.id,self.key,self.log_dir], shell=False,cwd=self.dir_path,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        img=subprocess.Popen(['../../../../algotrade_venv/Scripts/python', 'img_generation_engine.py',self.id,self.log_dir], shell=False,cwd=self.dir_path,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
       

        # To mark down the log time of img_generation log
        directory_log_img_generation = os.path.abspath(os.path.join(self.log_dir,'health_status_generating_graph_from_excel_continuously_stream' + str(self.id)+'.txt'))
        f = open(directory_log_img_generation,'w')
        f.write('This output log starts at ' + str(strftime("%a, %d %b %Y %H:%M:%S")) + '\n')
        f.close()


        # To mark down the log time of ffmpeg log
        directory_log_ffmpeg = os.path.join(self.log_dir,'ffmpeg_output_log_stream'+str(self.id)+'.txt')
        f = open(directory_log_ffmpeg,'w')
        f.write('This output log starts at ' + str(strftime("%a, %d %b %Y %H:%M:%S")) + '\n')
        f.close()



        print('FFmpeg is initiated! Wait for 60 seconds and start checking its health status')
        time.sleep(60)


        ## While true loop to monitor the health status of youtube stream by invoking youtube api and img_generation health by invoking img.poll() (subprocess.Popen().poll())
        while True:

            print('Time Now is '+  str(strftime("%a, %d %b %Y %H:%M:%S")))
            stream_status,health_status=youtube_health.get_youtube_stream_health() # call the get_youtube_stream_health() method for stream_status and health_status of youtube health

            if stream_status=='inactive':
                print('Youtube livestream is dead! Now restarting ffmpeg to restream.')
                youtube.kill() ## kill the youtube subprocess and restart a new one
                youtube=subprocess.Popen(['../../../../algotrade_venv/Scripts/python', 'youtube_streaming_engine.py',self.id,self.key,self.log_dir], shell=False,cwd=self.dir_path,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                print('FFmpeg is restarted, wait for 40 seconds to check for its health.')
                time.sleep(20)

            else:
                print('Youtube server is receiving data now. Its stream status now is '+ stream_status)
                print('Its health status is now '+ health_status)

            if img.poll() != None: # if img.poll() is None, subprocess is healthy
                print('Image generation engine has died, restarting after 8 seconds.')
                img.kill()
                time.sleep(8)
                img=subprocess.Popen(['../../../../algotrade_venv/Scripts/python', 'img_generation_engine.py',self.id,self.log_dir], shell=False,cwd=self.dir_path,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
            else:
                if stream_status != 'inactive':
                    print('Stream status of youtube for stream id '+str(self.id)+' is '+stream_status)
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
