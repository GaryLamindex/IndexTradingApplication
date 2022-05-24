from re import template
import plotly.graph_objects as go
import pandas as pd
import cufflinks as cf
import plotly.io as pio
from PIL import Image
import plotly.graph_objects as go

import six.moves.urllib
import json

class png():
    def heatmap(self):
        

        response = six.moves.urllib.request.urlopen(
            "https://raw.githubusercontent.com/plotly/datasets/master/custom_heatmap_colorscale.json"
        )
        cf.go_offline()
        dataset = json.load(response)
        fig = go.Figure()

        fig.add_trace(go.Heatmap(
            z=dataset["z"],
            colorscale=[[1.0, "rgb(100,5,18)"],
                        [0.0, "rgb(42,29,96)"]]
,
            ))     
        fig.update_layout(xaxis_showgrid=False,
                            xaxis_zeroline=False,
                            xaxis_showticklabels=False,
                            yaxis_showgrid=False, 
                            yaxis_zeroline=False,
                            yaxis_showticklabels=False,margin=dict(l=0, r=0, t=0, b=0)) 


        fig.update(layout_showlegend=False)

        

        directory = r'C:\Users\user\Desktop\heatmap_background.png'

        fig.write_image(file=directory,scale=1)
    
    def png(self):
        layout = dict(xaxis_showgrid=False,
                    xaxis_zeroline=False,
                    xaxis_showticklabels=False,
                    yaxis_showgrid=False, 
                    yaxis_zeroline=False,
                    yaxis_showticklabels=False,
                    paper_bgcolor='rgb(10,10,35)',
                    plot_bgcolor='rgb(10,10,35)')

        fig = go.Figure(data=[], layout=layout)
        directory = r'/home/ec2-user/python_project/pythonProject/engine/streaming_engine/png/Trial2.png'
        fig.write_image(file=directory,scale=1,width=740,height=950)

def main():
    test=png()
    test.heatmap()


        
if __name__ == "__main__":
    main()