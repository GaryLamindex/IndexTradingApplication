import boto3
from boto3.dynamodb.conditions import Key, Attr
import csv
import pandas as pd
import time
import datetime
import cufflinks as cf
import numpy as np
import warnings
import plotly
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import plotly.graph_objects as go
import plotly.express as px
from PIL import Image
import glob
import re
import os
import moviepy
from vidgear.gears import CamGear, WriteGear
from moviepy.editor import ImageSequenceClip
import sys
import stat
import io
import ctypes, sys
import kaleido
import json
import io
import pickle
import cv2
import ffmpeg
import imageio
import subprocess
from plotly.subplots import make_subplots
from time import gmtime, strftime
from io import BytesIO
import shlex


imagebytes=[]
# grab the dynamo_db_engine class
script_dir = os.path.dirname(__file__)
db_engine_dir = os.path.join(script_dir, '..', 'aws_engine')
sim_data_io_engine_dir= os.path.join(script_dir, '..', 'data_io_engine')
sys.path.append(db_engine_dir)
sys.path.append(sim_data_io_engine_dir)

import dynamo_db_engine as dynamo_db_engine
import sim_data_io_engine as sim_data_io_engine

class streaming_engine:
    def __init__(self):
        self.db = dynamo_db_engine.dynamo_db_engine("http://dynamodb.us-west-2.amazonaws.com")
        self.access_key= 'AKIAYDFQHWLX2FIG7W54'
        self.access_secrete= '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'
        self.bucket_name='python-youtube-videos-and-png-files'
        self.client_s3 = boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.access_secrete
        )
        self.get_table=sim_data_io_engine.sim_data_io_engine('streaming_test')

    def subplot(self,table1d):
        # for file in os.listdir("/home/ec2-user/streaming_engine/png/"):
        #     if file.endswith(".png"):
        #         os.remove("/home/ec2-user/streaming_engine/png/"+file)

        # time1m=pd.to_datetime(table1m['timestamp'].astype(int))
        time1d=pd.to_datetime(table1d['timestamp'].astype(int))
        time1d1d=list(table1d['timestamp'].astype(int))
        # Liquidity1m = list(table1m['Net Liquidation(Day End)'])
        cf.go_offline()
        Liquidity1d = list(table1d['Net Liquidation(Day End)'][-30:-1])
        time1d=list(table1d['timestamp'][-30:-1])
        fig=go.Figure()
        # fig = make_subplots(rows=2, cols=1,subplot_titles=('One day livestreaming','One month livestreaming'))
        # fig.update_yaxes(title_text="Net Liquidation(Day End)",row=1, col=1)
        # fig.update_yaxes(title_text="Net Liquidation(Day End)", row=2, col=1)
        fig.update_layout(paper_bgcolor='rgb(10,10,35)',plot_bgcolor='rgb(10,10,35)')
        fig.add_trace(go.Scatter(x=time1d, y=Liquidity1d,
    mode='lines+markers',name='lines+markers',line_color="rgb(106,74,255)"))
        # fig.add_trace(go.Scatter(x=time1m, y=Liquidity1m,
        #                     mode='lines+markers',
        #                         name='lines+markers',line_color="rgb(106,74,255)",),row=2,col=1)
        fig.update_layout(

                        yaxis_title='Net Liquidation(Day End)')
        fig.update_annotations(font=dict(
        family="Courier New, monospace",
        color="rgb(255,255,255)"
    ))
        string=''
        if round(Liquidity1d[-1]-Liquidity1d[-2],3) < 0:
            string = str(round(Liquidity1d[-1],3)) + '<br>' +str(round(Liquidity1d[-1]-Liquidity1d[-2],3)) + ' (\u2965' + str(abs(round((Liquidity1d[-1]-Liquidity1d[-2])/Liquidity1d[-2],3))) +'%)'
        elif round(Liquidity1d[-1]-Liquidity1d[-2],3) >0:
            string = str(round(Liquidity1d[-1],3)) + '<br>'  +'+ '+str(round(Liquidity1d[-1]-Liquidity1d[-2],3)) + ' (\u2963' + str(abs(round((Liquidity1d[-1]-Liquidity1d[-2])/Liquidity1d[-2],3))) +'%)'
        else:
            string = str(round(Liquidity1d[-1],3)) + '<br>' + str(round(Liquidity1d[-1]-Liquidity1d[-2],3)) + ' (0%)'
        fig.update_layout(title=string,font=dict(
        family="Courier New",
        color="rgb(106,74,255)"))


        # fig.add_annotation(text=string,font=dict(
        # family="Courier New",
        # color="RebeccaPurple" ),
        #         align='left',
        #         showarrow=False,
        #         xref='paper',
        #         yref='paper',
        #         x=-0.2,
        #         y=1.3)
        # directory = '/tmp/001.png'
        directory = '/tmp/001.png'
        print("image being overwritten")
        fig.write_image(file=directory,scale=4)
        # return plotly.io.to_image(fig)
        # pipe.stdin.write(fig.to_image(format='png',scale=4))
      
        # pipe.communicate(input=pipe.stdin)
        # pipe.close()
        # pipe.wait()
        # pipe.communicate()
        ##print(err)





def main():
    db = dynamo_db_engine.dynamo_db_engine("http://dynamodb.us-west-2.amazonaws.com")
    test=streaming_engine()
    date = strftime("%Y-%m-%d", gmtime())
    while True:
        date = strftime("%Y-%m-%d", gmtime())
        print('Current date is: '+ date)
        table= test.get_table.get_data_by_period(str(date),'1d')
        print('Table successfully retrieved.')
        test.subplot(table)
        # directory =test.subplot(table)
        # res, im_png = cv2.imencode('.png', directory )
        # ##img_str = cv2.imencode('.png', cv2.imread(directory))[1].toString()
        # pipe.stdin.write(im_png.tobytes())
        # pipe.close()
        # # pipe.kill()
        # pipe.communicate(input=str(test.subplot(table)))
        # pipe.kill()
        # # pipe.wait()
        time.sleep(10)