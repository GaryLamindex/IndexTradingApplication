import os
from time import gmtime, strftime
import time


def main():
    f=open('C://Users//user//Documents//GitHub//dynamodb_related//pythonProject//engine//streaming_engine//final_version//ffmpeg_output_log.txt','w')
    f.write('This output log starts at '+str(strftime("%a, %d %b %Y %H:%M:%S"))+'\n')
    f.close()
    cmd="ffmpeg -stream_loop -1 -re -f image2 -i C://Users//user//Documents//GitHub//dynamodb_related//pythonProject//engine//streaming_engine//final_version//test.png -re -stream_loop -1 -i C://Users//user//Documents//GitHub//dynamodb_related//pythonProject//engine//streaming_engine//final_version//002.mp3 -c:v libx264 -preset ultrafast -b:v 1M -bufsize 1700k -maxrate 1500k -r 1 -f flv rtmp://a.rtmp.youtube.com/live2/y7uu-df98-r0w9-qdtr-b128 >> ffmpeg_output_log.txt 2>&1"
    os.system(cmd)
    f=open('C://Users//user//Documents//GitHub//dynamodb_related//pythonProject//engine//streaming_engine//final_version//ffmpeg_output_log.txt','a')
    f.write('This output log ends at '+str(strftime("%a, %d %b %Y %H:%M:%S")))
    f.close()
        
if __name__ == "__main__":
    main()