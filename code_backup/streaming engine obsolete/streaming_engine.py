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

imagebytes=[]
# grab the dynamo_db_engine class
script_dir = os.path.dirname(__file__)
db_engine_dir = os.path.join(script_dir, '..', 'aws_engine')
get_sim_data_API_dir= os.path.join(script_dir, '..', 'data_io_engine')
sys.path.append(db_engine_dir)
sys.path.append(get_sim_data_API_dir)

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
        self.get_table=sim_data_io_engine.online_engine('backtest_rebalance_margin_wif_max_drawdown_control_0')
        self.df1dlength=0
        self.df6mlength=0
        ##self.startdate = time.time()
        ##self.sixmonthlowerend = startdate - 86400 * 30 * 3
        ##self.onedaylowerend = startdate - 86400

    def uploadtobukect2(self,filename):
        self.client_s3.upload_file(Filename=filename,Bucket=self.bucket_name,Key='hello')
        print('Successfully uploaded!')

    def uploadtobucket(self,obj,ky):
        self.client_s3.upload_fileobj(ACL='public-read',Body=obj,Bucket=self.bucket_name,Key=ky)

    def downloadfrombucket(self):
        self.client_s3.download_file(self.bucket_name,'index.jpg',r'D:\GIT REPO\video_file')
    
    def loadjson(self,ky):
        content_object = self.client_s3.get_object(Bucket=self.bucket_name, Key=ky)
        s1 = json.dumps(content_object, indent=4, sort_keys=True, default=str)
        d2 = json.loads(s1)
        return d2

    def listobjectbucket(self):
        response=self.client_s3.list_objects(Bucket=self.bucket_name)['Contents']
        return response['Key']

    def dataframeoutput(self):
        onedaydata = self.db.query_all_by_range('backtest_rebalance_margin_wif_max_drawdown_control_0',
                                           '0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0',
                                           'timestamp',
                                           (1508515200, 1508529540))
        sixmonthdata = self.db.query_all_by_range('backtest_rebalance_margin_wif_max_drawdown_control_0',
                                             '0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0',
                                             'timestamp',
                                             (1508515200, 1512142500))
        df6m = pd.DataFrame(sixmonthdata)
        df1d = pd.DataFrame(onedaydata)

        return df6m,df1d


    def fetchdata(self):
        startdate = time.time()
        sixmonthlowerend = startdate - 86400 * 30 * 3
        onedaylowerend = startdate - 86400

        currentdate = datetime.utcfromtimestamp(startdate).strftime('%d/%m/%Y')

   

        sixmonthdata=self.db.query_all_by_range('backtest_rebalance_margin_wif_max_drawdown_control_0',
                                                         '0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0',
                                                         'timestamp',
                                                         (1508515200,1508515260))

        onedaydata = self.db.query_all_by_range('backtest_rebalance_margin_wif_max_drawdown_control_0',
                                                           '0.038_rebalance_margin_0.01_maintain_margin_0.001max_drawdown__purchase_exliq_5.0',
                                                           'timestamp',
                                                           (1508515200, 1508515260))

        return sixmonthdata['Items'], onedaydata['Items']

    def pngproducer(self,table):
        # global s2i, s1i,dateticker
        # date = datetime.date(2017,10,20+dateticker)
        # date += datetime.timedelta(days=dateticker)
        # date = str(date)
        # table=self.get_table.get_data_1d(date)


        # # for i in range(5):
        # #     date += datetime.timedelta(days=1)
        # # s1i=''
        # # s2i=''
        # # full_table=self.fulltable()
        length=len(table)
        # png1d=[]
        # png6m=[]
        # imagelist=[]
        # duration=0
        for file in os.listdir("/home/ec2-user/python_project/pythonProject/engine/streaming_engine/png/"):
            if file.endswith(".png"):
                os.remove("/home/ec2-user/python_project/pythonProject/engine/streaming_engine/png/"+file)
        for i in range(0, length): ## Producing png files for one day

            time1d=pd.to_datetime(table['timestamp'].astype(int))
            Liquidity = list(table['NetLiquidation(Day End)'][0:i]) + [None] * (length - i)
            init_notebook_mode(connected=True)
            cf.go_offline()
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=time1d, y=Liquidity,
                                mode='lines+markers',
                                    name='lines+markers'))
            fig.update_layout(title='Livestream trading',

                        yaxis_title='Net Liquidation(Day End)')
            ##dict=fig.to_dict()
            directory = '/home/ec2-user/python_project/pythonProject/engine/streaming_engine/png/'
            if i >=100:
                a= str(i) +'.png'
            elif i<10:
                a= '00'+str(i) +'.png'
            else:
                a= '0' + str(i) + '.png'
            
            directory = directory + a
            fig.write_image(file=directory, scale=4)
    #     t=datetime.now().strftime("%Y%m%d%H%M%S")
    #     s1i= '/home/ec2-user/streaming_engine/mp4/'+ t + '.mp4'
    #   ##  s2i ='out_'+ t+'.mp4'
    #     s1= " ffmpeg -r 1 -i %03d.png -vcodec libx264 -crf 25 -pix_fmt yuv420p "+s1i
    #    ## s2 = 'ffmpeg -f lavfi -i aevalsrc=0 -i ' + t + '.mp4' + ' -c:v libx264 -c:a aac -pix_fmt yuv420p -shortest '+ s2i
    #     subprocess.call(s1, shell=True, cwd='/home/ec2-user/streaming_engine/png/')
    ##  subprocess.call(s2, shell=True, cwd='/home/ec2-user/streaming_engine/mp4/')
                    # imagelist.append(directory) 
        # t=datetime.now().strftime("%Y%m%d%H%M%S")
        # s1i= '/home/ec2-user/streaming_engine/mp4/'+ t + '.mp4'
        # s2i ='out_'+ t+'.mp4'
        # s1= " ffmpeg -r 1 -i %03d.png -vcodec libx264 -crf 25 -pix_fmt yuv420p "+s1i
        # s2 = 'ffmpeg -f lavfi -i aevalsrc=0 -i ' + t + '.mp4' + ' -c:v libx264 -c:a aac -pix_fmt yuv420p -shortest '+ s2i
        # subprocess.call(s1, shell=True, cwd='/home/ec2-user/streaming_engine/png/')
        # subprocess.call(s2, shell=True, cwd='/home/ec2-user/streaming_engine/mp4/')

    # def mp4toyoutube(self):
    #     cmdffmpeg='ffmpeg -re -stream_loop -1 -framerate 1 -i %03d.png -re -stream_loop -1 -i music.mp3 -c:v libx264 -preset ultrafast -r 1 -b:v 1500k -bufsize 3000k -maxrate 1500k -f flv rtmp://a.rtmp.youtube.com/live2/tr8p-6423-5ttv-ywac-a65p'
    #     subprocess.call(cmdffmpeg, shell=True, cwd='/home/ec2-user/streaming_engine/png/')
  
def main():
    db = dynamo_db_engine.dynamo_db_engine("http://dynamodb.us-west-2.amazonaws.com")
    test=streaming_engine()
    dateticker=0
    date = datetime.date(2017,10,18)
    while True:
        # date = datetime.date(2017,10,20+dateticker)
        # date += datetime.timedelta(days=dateticker)
        table= test.get_table.get_data_1d(str(date))
        length = len(table)
        if table.empty:
            if dateticker == 300:
                print('The table has reached the end. Programme is killed.')
                break
            else:
                string = 'There is no data on date ' + str(date)
                dateticker +=1
                date += datetime.timedelta(days=1)
                string = str(date) + 'is tried.'
                print(string)
        else:
            test.pngproducer(table)
            string= 'Successfully renewed png of ' + str(date) +'. Number of pngs created is: ' + str(length)+ '. Seconds to wait before renewing data is: ' + str(length)
            print(string)
            time.sleep(length)
            dateticker += 1
            date += datetime.timedelta(days=1)


 


if __name__ == "__main__":
    main()