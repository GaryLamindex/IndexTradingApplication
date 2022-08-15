import math
import os
from math import floor

import matplotlib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import MonthLocator


def plot_time_data_png(file_path, time_axis_name, data_axis_name, output_folder):
    df = pd.read_csv(file_path, low_memory=False)
    file_name = os.path.basename(file_path).split(".csv")[0]
    time_axis = df[time_axis_name]
    time_axis = pd.to_datetime(time_axis)
    data_axis = df[data_axis_name]

    fig, ax = plt.subplots()

    ax.set_title(file_name, fontsize=8)

    ax.xaxis.set_major_locator(MonthLocator(interval=6))
    # # ax.xaxis.set_minor_locator(MonthLocator(bymonthday=15))
    # # ax.xaxis.set_major_locator(matplotlib.dates.DayLocator(interval=365))
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%Y.%m'))
    fig.autofmt_xdate()

    data_diff = data_axis.max()-data_axis.min()
    pwr = math.pow(10, int("{:e}".format(data_diff).split("e+")[1]))
    step = floor(data_diff/ pwr)/20 * pwr
    tick_min = floor(data_axis.min() / pwr)*pwr - pwr*2
    tick_max = math.ceil(data_axis.max() / pwr) * pwr + pwr*2
    ax.set_yticks(np.arange(tick_min, tick_max, step=step))
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, pos: '{:}'.format(x / 1000000).split(".")[0] + 'M'))

    ax.set_xlabel(time_axis_name)
    ax.set_ylabel(data_axis_name)
    ax.plot(time_axis, data_axis)


    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
    png = f"{output_folder}/{file_name}.png"

    # plt.xticks(rotation=90)
    plt.grid(True)
    # plt.gcf().set_size_inches(100, 2)
    plt.savefig(png)
    print("done")
    pass


def plot_all_file_graph_png(folder_path, time_axis_name, data_axis_name, output_folder):
    directory = os.fsencode(folder_path)

    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            # print(f"{folder_path}/{filename}")
            # plot_time_data_png(f"{folder_path}/{filename}", time_axis_name, data_axis_name, output_folder)
            continue
        else:
            continue
    pass


def main():
    pass


    # path = "C:/Users/lam/Documents/GitHub/test_graph_data"
    # plot_all_file_graph_png(f"{path}", "date", "NetLiquidation", f"{path}/graph")


if __name__ == "__main__":
    main()