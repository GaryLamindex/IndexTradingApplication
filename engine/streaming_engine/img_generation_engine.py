import os
import plotly.graph_objects as go
import six.moves.urllib
import json
import cufflinks as cf
from openpyxl import load_workbook
import os
import win32com.client
from win32com.client import DispatchEx
from time import gmtime, strftime
import time
import csv
import sys
import xlwings as xw

class img_generation_engine:
    def __init__(self,stream_id,log_dir): # stream_id should be a positive integer, with zero included.
        self.id = stream_id
        self.log_dir=log_dir
        self.current_path=os.path.dirname(os.path.realpath(__file__))


    def generating_heatmap_background(self,rgb_left=(41,29,95),rgb_right=(5,5,18),width=1200,height=800): # stream_id means the stream id of youtube, can be 0,1,2,3,4...
                                                                                                          # rgb_right means top right corner's rgb colour, width and height is in pixel unit.
                                                                                                          # Input example: rgb_left = (0,3,4), rgb_right=(1,2,3),width =1200,height =800
        fig = go.Figure()                                                                                 # Setting up a figure object
        response = six.moves.urllib.request.urlopen(
            "https://raw.githubusercontent.com/plotly/datasets/master/custom_heatmap_colorscale.json"
        )
        dataset = json.load(response)
        cf.go_offline()
        rgb_right='rgb'+str(rgb_right)
        rgb_left='rgb'+str(rgb_left)
        fig.add_trace(go.Heatmap(
            z=dataset["z"],
            colorscale=[[1.0, rgb_right],   # 1.0 means top right corner
                        [0.0, rgb_left]]))  # 0.0 means bottom left corner

        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), xaxis_showgrid=False,
                          xaxis_zeroline=False,
                          xaxis_showticklabels=False,
                          yaxis_showgrid=False,
                          yaxis_zeroline=False,
                          yaxis_showticklabels=False)

        fig.update_traces(showscale=False)
        dir = os.path.join(self.current_path,'asset','stream_' + str(self.id))
        directory = os.path.join(dir,'background_colour_stream_' + str(self.id) + '.png')
        if os.path.exists(dir):                                                                           # Check if the stream
            fig.write_image(file=directory,scale=1,width=width,height=height)
        else:
            os.makedirs(dir)
            fig.write_image(file=directory, scale=1, width=width, height=height) # Writing the image to path .../stream_id directory
        print("The background image is produced successfully!")

    def generating_plain_colour(self,rgb=(41,41,41),width=1200,height=800): # rgb refers to the rgb colour of the output
                                                                            # Input example: rgb=(41,41,41),width=1200,height=800
        rgb='rgb'+str(rgb)
        layout = dict(xaxis_showgrid=False,                                 # To disable all the axis, ticks and grids to produce a plain colour image
                      xaxis_zeroline=False,
                      xaxis_showticklabels=False,
                      yaxis_showgrid=False,
                      yaxis_zeroline=False,
                      yaxis_showticklabels=False,
                      paper_bgcolor=rgb,
                      plot_bgcolor=rgb)

        fig = go.Figure(data=[], layout=layout)
        dir=os.path.join(self.current_path,'asset','stream_'+str(self.id))
        directory = os.path.join(dir,'plain_colour_stream_'+str(self.id)+'.png')
        if os.path.exists(dir):
            fig.write_image(file=directory, scale=1,width=width,height=height)
        else:
            os.makedirs(dir)
            fig.write_image(file=directory, scale=1, width=width, height=height)
        print("The plain colour image is produced successfully!")


    # This function produces updated chart from excel continuously
    def generating_graph_from_excel_continuously(self):
        directory_image = os.path.join(self.current_path,'asset','stream_' + str(self.id),'youtube_data_img_design_stream_'+str(self.id)+'.xlsm')
        directory_log = os.path.join(self.log_dir,'health_status_generating_graph_from_excel_continuously_stream' + str(self.id)+'.txt')
        directory_log=os.path.abspath(directory_log)
        directory_image=os.path.abspath(directory_image)
        wb=xw.Book(directory_image) # open the excel file which produces images 
        macro=wb.macro('Module1.save_file') # builds a macro object for that excel file


        while True:
            try:
                f = open(directory_log,'a')
                macro() ## excecute macro to produce a chart
                string = 'Successfully updated ' + 'at datetime: ' + str(strftime("%a, %d %b %Y %H:%M:%S")+'\n')
                print(string)
                f.write(string)
                f.close()
                time.sleep(20)
            except Exception as e:
                f = open(directory_log,'a')
                f.write(str(e)+'occured at time: ' + str(strftime("%a, %d %b %Y %H:%M:%S")))
                f.close()
                wb.close() # close the excel workbook
                wb.app.kill() # close the excel application
                time.sleep(20)
                self.generating_graph_from_excel_continuously() ## recursive call of this function
                
def main():

    engine = img_generation_engine(stream_id=sys.argv[1],log_dir=sys.argv[2])
    engine.generating_graph_from_excel_continuously()


if __name__ == "__main__":
    main()