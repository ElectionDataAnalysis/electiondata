import plotly.graph_objects as go
import os
import pathlib
from slugify import slugify


def plot(plot_type, data, fig_type, target_dir):
    labels, x, y = parse_data(data)
    fig = go.Figure()
    if plot_type == "scatter":
        fig.add_trace(
            go.Scatter(
                x=x,
                y=y,
                mode="markers",
                marker=go.scatter.Marker(size=10, color="blue"),
                hovertext=labels,
            )
        )
        fig.update_layout(
            xaxis_title=data["x"],
            yaxis_title=data["y"],
            font=dict(family="Courier New, monospace", size=18),
        )
        file_stem = slugify(f"scatter_{data['x-title']}_{data['y-title']}",
                            regex_pattern=r"[^A-z0-9-_]+",
                            lowercase=False,
                            )
    elif plot_type == "bar":
        total = [x + y for x, y in zip(x, y)]
        x_pct = [x / ttl for x, ttl in zip(x, total)]
        y_pct = [y / ttl for y, ttl in zip(y, total)]

        fig = go.Figure(
            data=[
                go.Bar(name=data["x"], x=labels, y=x_pct),
                go.Bar(name=data["y"], x=labels, y=y_pct),
            ]
        )
        fig.update_layout(
            title=dict(
                text=f"""{data["contest"]}<br>{data["subdivision_type"]}<br>{data["count_item_type"]}""",
                x=0.5,
                font=dict(family="Courier New, monospace", size=18),
            ),
            barmode="group",
            font=dict(family="Courier New, monospace", size=14),
        )
        file_stem = slugify(f"bar_{data['x']}_{data['y']}_{data['count_item_type']}_{data['contest']}",
                            regex_pattern=r"[^A-z0-9-_]+",
                            lowercase=False,
                            )
    image_dir = os.path.join(target_dir, "images")
    file_name = f"{file_stem}.{fig_type}"
    file_path = os.path.join(image_dir, file_name)

    if not os.path.isdir(image_dir):
        path = pathlib.Path(image_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
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
    for result in data["counts"]:
        labels.append(result["name"])
        x.append(result["x"])
        y.append(result["y"])
    return labels, x, y
