import subprocess
cmd= ['ffmpeg',
'-stream_loop','-1',
# '-f','image2pipe',
# '-vcodec','png',
# '-y','',
#  '-readrate','1',
# '-r','1',
# '-f','image2pipe',
# '-vcodec','qtrle',
# '-c:v', 'libx264',
# '-codec','copy',
'-i','/home/ec2-user/python_project/pythonProject/engine/streaming_engine/001.png',
# '-stream_loop','-1',
# '-c:v', 'libx264',
# '-readrate','1',
# '-acodec','aac',    
# '-codec','copy',
'-stream_loop','-1',
'-i','/home/ec2-user/python_project/pythonProject/engine/streaming_engine/004.mp3',
'-c:v', 'libx264',
##'-x264-params','keyint=10:scenecut=0', ##You'll need to reencode. Set x264's keyint parameter to 5*fps and disable scenecut. If your fps is 24 for example :
'-c:a','aac',
'-preset', 'medium',
# # '-r',' 1',
'-b:v', '1500k',
'-b:a','1500k',
'-bufsize','3000k',
# '-r','1',
'-maxrate','1500k',
# '-vcodec','qtrle',
# '-framerate','1',
# '-vcodec','libx264',
'-pix_fmt', 'yuv420p', 
'-f', 'flv',
'-framerate','3',
'rtmp://a.rtmp.youtube.com/live2/e9b3-ccf3-yt7c-19hm-9tb6']
subprocess.run(cmd)
col = ["date", "time", "res"]
res_str = get_res()
date = datetime.now().date
time = datetime.now().time

