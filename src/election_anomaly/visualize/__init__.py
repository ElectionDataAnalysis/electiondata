import plotly
import plotly.graph_objects as go
import numpy as np
import os

def plot_scatter(data, fig_type, target_dir):
    labels, x, y = parse_data(data)
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=x,
        y=y,
        mode="markers",
        marker=go.scatter.Marker(
            size=10,
            color="blue"
        ),
        hovertext=labels
    ))
    fig.update_layout(
        xaxis_title=data["x"],
        yaxis_title=data["y"],
        font=dict(
            family="Courier New, monospace",
            size=18
        )
    )
    image_dir = os.path.join(target_dir, 'images')
    file_name = f'{data["x"].replace(" ", "-")}_{data["y"].replace(" ", "-")}.{fig_type}'
    file_path = os.path.join(image_dir, file_name)

    if not os.path.isdir(image_dir):
        os.mkdir(image_dir)

    if fig_type == "html":
        fig.write_html(file_path)
    else:
        fig.write_image(file_path)
    fig.show()


def parse_data(data):
    """returns a list of labels, x, and y data from the standard JSON format
    returned by the analyze functions"""
    labels = []
    x = []
    y = []
    for key in data['counts']:
        labels.append(key)
        x.append(data['counts'][key]['x'])
        y.append(data['counts'][key]['y'])
    return labels, x, y