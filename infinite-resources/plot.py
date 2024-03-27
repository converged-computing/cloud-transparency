#!/usr/bin/env python3

import argparse
import collections
import fnmatch
import os
import json

import matplotlib.pyplot as plt
import pandas
import seaborn as sns

plt.style.use("bmh")
here = os.path.dirname(os.path.abspath(__file__))


def get_parser():
    parser = argparse.ArgumentParser(
        description="Plot Instance Creation Times",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--results",
        help="directory with raw results data",
        default=os.path.join(here, "data"),
    )
    parser.add_argument(
        "--out",
        help="directory to save parsed results",
        default=os.path.join(here, "img"),
    )
    return parser


def recursive_find(base, pattern="*.*"):
    """
    Recursively find and yield files matching a glob pattern.
    """
    for root, _, filenames in os.walk(base):
        for filename in fnmatch.filter(filenames, pattern):
            yield os.path.join(root, filename)


def find_inputs(input_dir):
    """
    Find inputs (results files)
    """
    files = []
    for filename in recursive_find(input_dir, pattern="*.json"):
        # We only have data for small
        files.append(filename)
    return files


def read_json(path):
    with open(path, "r") as fd:
        content = json.loads(fd.read())
    return content


def main():
    """
    Run the main plotting operation!
    """
    parser = get_parser()
    args, _ = parser.parse_known_args()

    # Output images and data
    outdir = os.path.abspath(args.out)
    indir = os.path.abspath(args.results)
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    # Find input files (skip anything with test)
    files = find_inputs(indir)
    if not files:
        raise ValueError(f"There are no input files in {indir}")

    # This does the actual parsing of data into a formatted variant
    # Has keys results, iters, and columns
    df = parse_data(files)
    df.to_csv(os.path.join(outdir, "instance-times.csv"))
    plot_results(df, outdir)


def plot_results(df, outdir):
    """
    Plot lammps results
    """
    # Plot each!
    types = list(df.nodes.unique())
    types.sort()
    tags = list(df.tag.unique())
    tags.sort()

    # ALWAYS double check this ordering, this
    # is almost always wrong and the colors are messed up
    colors = sns.color_palette("hls", 16)
    hexcolors = colors.as_hex()
    palette_tags = collections.OrderedDict()
    for t in tags:
        palette_tags[t] = hexcolors.pop(0)

    colors = sns.color_palette("hls", 16)
    hexcolors = colors.as_hex()
    palette = collections.OrderedDict()
    for t in types:
        palette[t] = hexcolors.pop(0)

    for instance in df.instance.unique():
        subset = df[df.instance == instance]
        make_plot(
            subset,
            title=f"Node Group Creation Times for Instance {instance}",
            tag=f"{instance}_creation_times",
            ydimension="create_nodes",
            xdimension="nodes",
            palette=palette,
            outdir=outdir,
            ext="png",
            plotname=f"{instance}_deletion_times",
            hue="nodes",
            plot_type="bar",
            xlabel="Size (nodes)",
            ylabel="Time (seconds)",
        )
        make_plot(
            subset,
            title=f"Node Group Deletion Times for Instance {instance}",
            tag=f"{instance}_deletion_times",
            ydimension="delete_nodes",
            xdimension="nodes",
            palette=palette,
            outdir=outdir,
            ext="png",
            plotname=f"{instance}_deletion_times",
            hue="nodes",
            plot_type="bar",
            xlabel="Size (nodes)",
            ylabel="Time (seconds)",
        )

        make_plot(
            subset,
            title=f"Node Group Creation Times for Instance {instance} by time of day",
            tag=f"{instance}_creation_times_time_of_day",
            ydimension="create_nodes",
            xdimension="nodes",
            palette=palette_tags,
            outdir=outdir,
            ext="png",
            plotname=f"{instance}_deletion_times_time_of_day",
            hue="tag",
            plot_type="bar",
            xlabel="Size (nodes)",
            ylabel="Time (seconds)",
        )
        make_plot(
            subset,
            title=f"Node Group Deletion Times for Instance {instance} by time of day",
            tag=f"{instance}_deletion_times_time_of_day",
            ydimension="delete_nodes",
            xdimension="nodes",
            palette=palette_tags,
            outdir=outdir,
            ext="png",
            plotname=f"{instance}_deletion_times_time_of_day",
            hue="tag",
            plot_type="bar",
            xlabel="Size (nodes)",
            ylabel="Time (seconds)",
        )


def parse_data(files):
    """
    Given a listing of files, parse into results data frame
    """
    # Parse into data frame
    df = pandas.DataFrame(
        columns=[
            "uid",
            "instance",
            "nodes",
            "create_nodes",
            "delete_nodes",
            "tag",
            "date",
        ]
    )
    idx = 0

    for filename in files:
        parsed = os.path.relpath(filename, here)
        pieces = parsed.split(os.sep)
        tag = pieces[-3]
        date = pieces[-4]

        item = read_json(filename)

        # We just care about total wall time
        for uid, datum in item["experiments"].items():
            if "create_cluster_nodes" not in datum["times"]:
                continue
            df.loc[idx, :] = [
                datum["id"],
                datum["machine_type"],
                datum["size"],
                datum["times"]["create_cluster_nodes"],
                datum["times"]["delete_nodegroup"],
                tag,
                date,
            ]
            idx += 1

    return df


def make_plot(
    df,
    title,
    tag,
    ydimension,
    xdimension,
    palette,
    xlabel,
    ylabel,
    ext="pdf",
    plotname="lammps",
    plot_type="violin",
    hue="experiment",
    outdir="img",
):
    """
    Helper function to make common plots.
    """
    plotfunc = sns.boxplot
    if plot_type == "violin":
        plotfunc = sns.violinplot

    ext = ext.strip(".")
    plt.figure(figsize=(24, 12))
    sns.set_style("dark")
    ax = plotfunc(
        x=xdimension,
        y=ydimension,
        hue=hue,
        data=df,
        linewidth=0.8,
        palette=palette,
        whis=[5, 95],
    )

    plt.title(title)
    ax.set_xlabel(xlabel, fontsize=16)
    ax.set_ylabel(ylabel, fontsize=16)
    ax.set_xticklabels(ax.get_xmajorticklabels(), fontsize=14)
    ax.set_yticklabels(ax.get_yticks(), fontsize=14)
    plt.savefig(os.path.join(outdir, f"{tag}_{plotname}.{ext}"))
    plt.clf()


if __name__ == "__main__":
    main()
