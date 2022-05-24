import datetime
import sys
import subprocess
import os
import time
from time import gmtime, strftime

current_dir = os.path.dirname(__file__)
application_dir = os.path.join(current_dir, 'application') ##refers to the application directory, which stores streaming_application.py and realtime_application.py


# Takes in an arbitary number of keyword arguments and pass into streaming_application.py as arguments
def main(**kwargs):

    ## Check if the log_directory exists, if not create one
    if os.path.exists(log_dir):
        pass
    else:
        os.makedirs(log_dir)


    # To open a subprocess for streaming_application.py, str(kwargs) is passed into streaming_application.py
    engine_streaming=subprocess.Popen(['../../algotrade_venv/Scripts/python','streaming_application.py',str(kwargs)], shell=False,cwd=application_dir) # For subprocess.Popen, it is recommended to take shell=False for security reason. cwd refers to the directory you want to excecute the commandline
    
    # Create an output log and error log for realtime_application and registers time
    dir_realtime_log_output=os.path.join(log_dir,'realtime_application_log_output.txt')                                                            
    dir_realtime_log_error=os.path.join(log_dir,'realtime_application_log_error.txt')
    log_output = open(dir_realtime_log_output, 'w')
    log_error= open(dir_realtime_log_error, 'w')
    log_output.write('This output log starts at ' + str(strftime("%a, %d %b %Y %H:%M:%S")) + '\n') # Write down the starting time for real_time_trade_log.txt
    log_error.write('This output log starts at ' + str(strftime("%a, %d %b %Y %H:%M:%S")) + '\n')
    
    # To open a subprocess for realtime_application.py
    engine_realtime_trade=subprocess.Popen(['../../algotrade_venv/Scripts/python','realtime_application.py'], shell=False,cwd=application_dir,stdout=log_output, stderr=log_error)

    while True:

        # To monitor if realtime_trade application child process is running
        # Please refer to this documentation for subprocess.Popen() method https://docs.python.org/3/library/subprocess.html#subprocess.Popen

        print('The status of realtime_trade application is: '+str(engine_realtime_trade.poll())) # subprocess.Popen.poll() returns None if the subprocess is running. Returns 1 if it stops running

        engine_realtime_trade.stdout # Reads subprocess's standard output(stdout) to log file
        engine_realtime_trade.stderr # Reads subprocess's standard error(stderr) to log file

        time.sleep(10)




if __name__ == "__main__":

    # Example of input:
    # main(stream_0=[0,'u7m4-y3y6-xs1z-saha-1ke8',log_dir],stream_1=[1,'evj7-p5zz-rsxk-p8m9-04bx',log_dir]) 
    # Default Stream Key (RTMP, Variable) 
    log_dir=sys.argv[1]
    main(stream_0=[0,'kts2-yda5-d5pe-r3me-8uay',log_dir])  