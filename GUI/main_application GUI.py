from tkinter import *
import main_application
import subprocess
from time import gmtime, strftime
import os
import time

ts=time.time()
current_dir = os.path.dirname(__file__)
log_dir=os.path.join(current_dir, '../application', 'log', 'log_of_start_time_' + str(ts))


log_dir_cmd='start '+log_dir

application=None

root = Tk()
root.title('Lam Index Invest Trading Application')

def button_run():
    ## Check if the log_directory exists, if not create one
    if os.path.exists(log_dir):
        pass
    else:
        os.makedirs(log_dir)
    dirdir=os.path.join(log_dir,'main_application_log.txt')
    log_out=open(dirdir,'w')
    log_out.write('This output log starts at ' + str(strftime("%a, %d %b %Y %H:%M:%S")) + '\n')
    application = subprocess.Popen(['../../algotrade _venv/Scripts/python','main_application.py',log_dir], shell=False,stdout=log_out,stderr=log_out)



def directory_window():
    os.system(log_dir_cmd)

def quit():
    root.quit()


button_run = Button(root,text='Run Application',padx=100,pady=20,command=button_run) 
button_run.grid(row=2,column=0,columnspan=8)

button_kill = Button(root,text='Stop Application',padx=100,pady=20,command=quit) 
button_kill.grid(row=3,column=0,columnspan=8)


button_log_dir= Button(root,text='Log Directory',padx=100,pady=20,command=directory_window)
button_log_dir.grid(row=4,column=0,columnspan=8)

root.mainloop()
