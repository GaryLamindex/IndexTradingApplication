import plotly.graph_objects as go
import six.moves.urllib
import json
import cufflinks as cf



fig = go.Figure()

response = six.moves.urllib.request.urlopen(
            "https://raw.githubusercontent.com/plotly/datasets/master/custom_heatmap_colorscale.json"
        )

dataset = json.load(response)
cf.go_offline()
fig.add_trace(go.Heatmap(
    z=dataset["z"],
    colorscale=[[1.0, "rgb(0,0,0)"], # 1.0 means top right corner
                [0.0, "rgb(200,200,200)"]])) # 0.0 means bottom left corner

fig.update_layout(margin=dict(l=0, r=0, t=0, b=0),xaxis_showgrid=False,
                    xaxis_zeroline=False,
                    xaxis_showticklabels=False,
                    yaxis_showgrid=False,
                    yaxis_zeroline=False,
                    yaxis_showticklabels=False)
fig.update_traces(showscale=False)


directory = r'C:\Users\user\Desktop\heatmap_background.png'

fig.write_image(file=directory,scale=1,width=1200,height=800)