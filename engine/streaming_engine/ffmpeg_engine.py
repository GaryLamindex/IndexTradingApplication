import subprocess
import time
import sys
import os
from time import gmtime, strftime

## Explanation for youtube livestreaming
## youtube livestreaming can be done via rtmp protocol and HLS protocol. RTMP protocol is used and recommended for this script. For rtmp protocol, it takes a stream key for streaming. The format is 'rtmp://a.rtmp.youtube.com/live2/'stream_key' 
## e.g. rtmp://a.rtmp.youtube.com/live2/u7m4-y3y6-xs1z-saha-1ke8. One stream key is for one unique stream. E.g. u7m4-y3y6-xs1z-saha-1ke8 may refer to Gary Lam Index Invest Live Stream streaming event. Stream keys can be make in youtube livestreaming website

## Explanation for ffmpeg
## FFmpeg is a free and open-source software project consisting of a suite of libraries and programs for handling video, audio, and other multimedia files and livestreaming. In this PC(Cyberport desktop), FFmpeg is located at C:\Program Files (x86)\FFmpeg\bin
## To use FFmpeg, just copy the file named FFmpeg under directory C:\Program Files (x86) to any PC you like, then add the pathway ...\FFmpeg\bin to PATH environment variable
## FFmpeg takes in a number of arguments specified by different flags. For example for command line 'ffmpeg -r 1 -i abc.mp4 abc.avi', argument -i abc.mp4 means taking abc.mp4 as input, -r 1 means reading abc.mp4 in a readrate of 1.
## you can see that there is not flag(e.g. -r , -i) before abc.avi. It means that it is an output. There should only be one unflagged argument, and that is the output file.
## There are many important flags for this livestreaming project, for example, -re refers to reading input source at its own rate, and it takes no argument. -stream_loop refers to how many loops you want to repeat the command line, for -1 argument, it refers
## to loop indefinitely. As for -f fmt (input/output), it forces input or output file format. The format is normally auto detected for input files and guessed from the file extension for output files.
## There are also codecs(encoders and decoders for regulating encoding input streams and decoding output streams.) For this streaming scripts, we use libx264 for -c:v (codec for video) and none for -c:a(codec for audio).
## One last important note it that a ffmpeg commandline is divided into four main parts. For example, for the following code
## 'ffmpeg -stream_loop -1 -re -f image2 -i test.png -re -stream_loop -1 -i 002.mp3 -c:v libx264 -preset ultrafast -pix_fmt yuv420p -b:v 1M -bufsize 1700k -maxrate 1500k -r 1 -f flv rtmp://a.rtmp.youtube.com/live2/y7uu-df98-r0w9-qdtr-b128'
## -stream_loop -1 -re -f image2 -i test.png means specifying -stream_loop -1 -re -f image2 for first input test.png, -re -stream_loop -1 -i 002.mp3 means specifying -re -stream_loop -1 for secondinput 002.mp3. FFmpeg commandline can take in an arbitary number of inputs
## -c:v libx264 -preset ultrafast -pix_fmt yuv420p -b:v 1M -bufsize 1700k -maxrate 1500k means specifying the codecs and input output bit rates and buffer size
## -r 1 -f flv rtmp://a.rtmp.youtube.com/live2/y7uu-df98-r0w9-qdtr-b128 means specifying the output format at a output readrate of 1 with output format in flv. The output source is rtmp://a.rtmp.youtube.com/live2/y7uu-df98-r0w9-qdtr-b128.
## Please refer to ffmpeg documentation for better understanding https://www.ffmpeg.org/ffmpeg.html

## Some video processing terminology: buffer size: size of data that is reserved for processor to process data from input to output. The lower the buffer size, the higher the transmission speed between input and output, at the expense of lower processing quality.
## bitrate: how much data being transmitted to server. Unit is Mbps(Mb per second). The lower the bitrate, the lower the quality of the output.
## Resolution: the number of pixels a video contains. e.g. 1080p means 1920 x 1080 pixels. High resolution does not necessarily guarantee good bitrate. High resolution
## Bandwidth and internet speed. Latency refers to the lag you experience while waiting for something to load. 
## Video encoding and decoding: please refer to https://www.haivision.com/blog/all/the-beginners-guide-to-video-encoding-decoding-and-transcoding/
## Encoding vs. Transcoding
## Even experts often use the terms encoding and transcoding interchangeably, but they aren’t the same process. 
## Where encoding deals with compression of raw data, transcoding translates an existing video stream/file into a different format. 
## In other words, transcoding re-encodes a previously encoded stream/file using a different set of rules. 
## The distinction is subtle but important, especially for streaming.

## Youtube livestreaming and ffmpeg: please set a higher bit rate for youtube live streaming because youtube will compress whatever is streamed to reduce file size.


# this function takes in the second argument of command line for id and third argument for stream_key. Positioning of argument is: e.g. 'python ffmpeg_engine.py 0 abcd-efgh'.
# For the above command line, first argument of python is ffmpeg_engine.py, second is 0 and so forth 
def main():
    id=sys.argv[1] 
    stream_key= sys.argv[2]
    log_dir=sys.argv[3]
    current_path=os.path.dirname(os.path.realpath(__file__))
    directory_log=os.path.join(log_dir,'ffmpeg_output_log_stream'+str(id)+'.txt') # Locate the directory for log file, audio.mp3 etc
    directory_image=os.path.join(current_path,'asset','stream_'+str(id),'upload_youtube_png_'+str(id)+'.png')
    directory_audio=os.path.join(current_path,'asset','audio.mp3')

    log_file = open(directory_log,'w') 

    rtmp='rtmp://a.rtmp.youtube.com/live2/'+stream_key 
    cmd = "ffmpeg -stream_loop -1 -re -f image2 -i "+ directory_image+' -re -stream_loop -1 -i '+directory_audio+' -crf 23 -c:v libx264 -preset ultrafast -b:v 2M -bufsize 1000k -maxrate 2M -f flv '+rtmp
    ffmpeg=subprocess.Popen(cmd, shell=False,stdout=log_file, stderr=log_file)
    while True:
        ffmpeg.stdout # Reads subprocess's standard output(stdout) to log file every 10 seconds
        ffmpeg.stderr # Reads subprocess's standard error(stderr) to log file every 10 seconds
        time.sleep(10)
        
if __name__ == "__main__":
    main()