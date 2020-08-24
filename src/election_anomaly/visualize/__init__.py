import plotly
import plotly.graph_objects as go
import numpy as np
import os

def plot(type, data, fig_type, target_dir):
    labels, x, y = parse_data(data)
    fig = go.Figure()
    if type == 'scatter':
        fig.add_trace(go.Scatter(
            x=x,
            y=y,
            mode='markers',
            marker=go.scatter.Marker(
                size=10,
                color='blue'
            ),
            hovertext=labels
        ))
        fig.update_layout(
            xaxis_title=data['x'],
            yaxis_title=data['y'],
            font=dict(
                family='Courier New, monospace',
                size=18
            )
        )
    elif type == 'bar':
        #labels might get long, so we grab the most granular portion:
        # for i in range(0, len(labels)):
        #     l = labels[i].split(';')[-1]
        #     labels[i] = l

        #sort by most anomalous
        # anomalous = list(map(abs, scores))
        # to_sort = list(zip(anomalous, labels, x, y))
        # ordered = sorted(to_sort, reverse=True)
        # anomalous, labels, x, y = list(zip(*ordered))
        # labels = list(labels)
        # x = list(x)
        # y = list(y)

        fig = go.Figure(
            data=[
                go.Bar(name=data['x'], x=labels, y=x),
                go.Bar(name=data['y'], x=labels, y=y)
            ]
        )
        fig.update_layout(
            title=dict(
                text=f'''{data['contest']}<br>{data['subdivision_type']}<br>{data['count_item_type']}''',
                x = 0.5,
                font=dict(
                   family='Courier New, monospace',
                   size=18 
                )
            ),
            barmode='group',
            font=dict(
                family='Courier New, monospace',
                size=14
            )
        )
    image_dir = os.path.join(target_dir, 'images')
    file_name = f'{data["x"].replace(" ", "-")}_{data["y"].replace(" ", "-")}.{fig_type}'
    file_path = os.path.join(image_dir, file_name)

    if not os.path.isdir(image_dir):
        os.mkdir(image_dir)

    if fig_type == 'html':
        fig.write_html(file_path)
    else:
        fig.write_image(file_path)
    fig.show()


def plot_bar(data, fig_type, target_dir):
    labels, x, y = parse_data(data)
    fig = go.Figure(
        data=[
            go.Bar(name=data['x'], x=[labels[0]], y=x),
            go.Bar(name=data['y'], x=[labels[0]], y=y)
        ]
    )
    fig.update_layout(
        barmode='group',
        font=dict(
            family='Courier New, monospace',
            size=18
        )
    )
    image_dir = os.path.join(target_dir, 'images')
    file_name = f'{data["x"].replace(" ", "-")}_{data["y"].replace(" ", "-")}.{fig_type}'
    file_path = os.path.join(image_dir, file_name)

    if not os.path.isdir(image_dir):
        os.mkdir(image_dir)

    if fig_type == 'html':
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
    for result in data['counts']:
        labels.append(result['name'])
        x.append(result['x'])
        y.append(result['y'])
    return labels, x, y
