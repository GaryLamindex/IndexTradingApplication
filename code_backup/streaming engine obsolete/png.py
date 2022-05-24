import plotly.graph_objects as go
import pandas as pd
import cufflinks as cf
import plotly.io as pio
from PIL import Image
import plotly.graph_objects as go

import six.moves.urllib
import json

class png():
    def heatmap(self,table):
        

        response = six.moves.urllib.request.urlopen(
            "https://raw.githubusercontent.com/plotly/datasets/master/custom_heatmap_colorscale.json"
        )

        dataset = json.load(response)

        fig = go.Figure()

        fig.add_trace(go.Heatmap(
            z=dataset["z"],
            colorscale=[[0.0, "rgb(165,0,38)"],
                        [0.1111111111111111, "rgb(215,48,39)"],
                        [0.2222222222222222, "rgb(244,109,67)"],
                        [0.3333333333333333, "rgb(253,174,97)"],
                        [0.4444444444444444, "rgb(254,224,144)"],
                        [0.5555555555555556, "rgb(224,243,248)"],
                        [0.6666666666666666, "rgb(171,217,233)"],
                        [0.7777777777777778, "rgb(116,173,209)"],
                        [0.8888888888888888, "rgb(69,117,180)"],
                        [1.0, "rgb(49,54,149)"]]
))


        # trace = go.Heatmap(z=matrix,
        #            x=ticks,
        #            y=ticks,
        #            colorscale='Viridis',
        #            showscale=False)

        # layout = dict(xaxis_showgrid=False,
        #       xaxis_zeroline=False,
        #       xaxis_showticklabels=False,
        #       yaxis_showgrid=False, 
        #       yaxis_zeroline=False,
        #       yaxis_showticklabels=False)

        # fig = go.Figure(data=trace, layout=layout)
        # fig.show()
  
        directory = '/home/ec2-user/python_project/pythonProject/engine/streaming_engine/aaa.jpg'

        fig.write_image(file=directory,scale=4)