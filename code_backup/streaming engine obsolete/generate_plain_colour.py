import plotly.graph_objects as go
import pandas as pd
import cufflinks as cf
import plotly.io as pio
from PIL import Image
import plotly.graph_objects as go

import six.moves.urllib
import json

class png:
    def plain_colour(self):
        layout = dict(xaxis_showgrid=False,
                    xaxis_zeroline=False,
                    xaxis_showticklabels=False,
                    yaxis_showgrid=False, 
                    yaxis_zeroline=False,
                    yaxis_showticklabels=False,
                    paper_bgcolor='rgb(233,233,233)')

        fig = go.Figure(data=[], layout=layout)
        directory = r'C:\Users\user\Documents\GitHub\dynamodb_related\pythonProject\engine\streaming_engine\final_version\plain_colour_photo.png'
        fig.write_image(file=directory)

def main():
    test=png()
    test.plain_colour()


        
if __name__ == "__main__":
    main()