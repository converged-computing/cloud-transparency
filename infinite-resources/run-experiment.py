#!/usr/bin/env python3

import argparse
import copy
import json
import os
import time
import sys
from datetime import datetime

from kubescaler.scaler.google import GKECluster

# import the script we have two levels up
here = os.path.abspath(os.path.dirname(__file__))
root = os.path.dirname(here)

cli = None

# These are the instances and sizes we are going to try to select
# QUESTION: how do we do these experiments if we can't even get quota?
experiment_plans = [
    {
        "instance": "c3-standard-8",
        "sizes": {"min": 10, "max": 150, "increment": 10},
    },
    {
        "instance": "c2d-standard-56",
        "sizes": {"min": 10, "max": 150, "increment": 10},
    },
]


def get_parser():
    parser = argparse.ArgumentParser(
        description="Infinite Instance Tester",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--data-dir",
        help="path to save data",
        default=os.path.join(here, "data"),
    )
    parser.add_argument(
        "--tag",
        help="tag for data (e.g., time of day",
    )
    parser.add_argument(
        "-n",
        "--name",
        help="select one or more experiments by name (instance)",
        action="append",
    )
    parser.add_argument(
        "--zone",
        help="Zone to request resources for (e.g., us-central1-a).",
        default="us-central1-a",
    )
    parser.add_argument(
        "--region",
        help="Region to request resources for (e.g., us-central1). Be careful, as this often means getting them across zones.",
    )
    parser.add_argument(
        "--cluster-name",
        help="cluster name to use (defaults to spot-instance-testing-cluster",
        default="resources-testing-cluster",
    )
    parser.add_argument(
        "--project",
        help="Google cloud project name",
        default="llnl-flux",
    )
    return parser


class Experiment:
    """
    An Experiment holds a configuration for a scaling test.
    """

    def __init__(self, plan):
        self.sizes = set()
        self.load(plan)

    def load(self, plan):
        """
        Load (or reload) an experiment into the class.
        """
        self.plan = plan

        # Ideally, the user provides a name. Otherwise, long UID
        # The uid just smashes all the fields together
        self.id = self.plan.get("instance") or generate_uid(plan)
        sizes = self.plan["sizes"]

        # Expand spec into sizes for experiment
        if "min" not in sizes or "max" not in sizes or "increment" not in sizes:
            sys.exit(
                f"Experiment {self.id} is missing one of sizes.min, max, or increment."
            )

        for i in range(sizes["min"], sizes["max"], sizes["increment"]):
            self.sizes.add(i)

        # Always make sure we have the max size
        self.sizes.add(sizes["max"])

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f"{self.id}.Experiment"

    @property
    def machine_type(self):
        return self.plan.get("instance")

    def export(self):
        """
        Export experiment metadata
        """
        return {"plan": self.plan}


def read_json(filename):
    """
    Read json from file.
    """
    with open(filename, "r") as fd:
        content = json.loads(fd.read())
    return content


def confirm_action(question):
    """
    Ask for confirmation of an action
    """
    response = input(question + " (yes/no)? ")
    while len(response) < 1 or response[0].lower().strip() not in "ynyesno":
        response = input("Please answer yes or no: ")
    if response[0].lower().strip() in "no":
        return False
    return True


def plan_experiments(args):
    """
    Given experiment "plans" create a matrix of actual experiments (and instance types) to run
    """
    experiments = {}
    for plan in experiment_plans:
        # The user can select an experiment by name (instance type)
        if args.name and plan["instance"] not in args.name:
            continue

        print(f"Planning experiments for {plan}")
        exp = Experiment(plan)

        # Save us from ourselves, don't repeat an experiment
        if exp.id in experiments:
            continue

        experiments[exp.id] = exp
    return experiments


def generate_uid(params):
    """
    Generate a unique id based on params.
    """
    uid = ""
    for k, v in params.items():
        if not isinstance(v, dict):
            uid += k.lower() + "-" + str(v).lower()
        else:
            uid += k.lower()
    return uid


def write_json(obj, filename):
    """
    write json to output file
    """
    with open(filename, "w") as fd:
        fd.write(json.dumps(obj, indent=4))


def run_experiments(experiments, args):
    """
    Wrap experiment running separately in case we lose spot nodes and can recover
    """
    global cli

    # Use kubescaler gkecluster to create the cluster
    # We will request / delete nodegroups with spot from it
    cli = GKECluster(
        project=args.project,
        # This is the sticky node to install operators to
        # It can be small
        machine_type="e2-standard-4",
        name=args.cluster_name,
        # The client will generalize this to location (and we expect one or the other)
        # preference is given to zone, as it is more specific
        region=args.region,
        zone=args.zone,
        # The main cluster node pool "default-pool" should just have one node.
        # We will try add named node groups for experiments to it
        node_count=1,
        min_nodes=1,
        max_nodes=1,
        max_vcpu=4,
        max_memory=16,
        # Labels just for the sticky node!
        labels={"sticky": "yes"},
    )

    # This creates a single node for the control plane
    # We will add machine types as node groups (to create and delete from the cluster) later
    cli.create_cluster()

    # This is cheating a bit, I couldn't get the cert manager installed
    res = os.system(
        f"gcloud container clusters get-credentials {cli.cluster_name} --location={cli.location}"
    )
    if res != 0:
        print("Issue getting kube config credentials, debug!")
        import IPython

        IPython.embed()

    # Save results here as we go
    # Note that this can store multiple experiments, but for data parsing
    # and being conservative I'm running them individually
    results = copy.deepcopy(cli.times)
    results["experiments"] = {}
    results["start_time"] = str(datetime.now())
    results["cluster_name"] = args.cluster_name

    # Get the date for the result too
    now = datetime.now()
    today = f"{now.year}-{now.month}-{now.day}"

    # Note that the experiment already has a table of values filtered down
    # Each experiment has some number of batches (we will typically just run one experiment)
    for name, exp in experiments.items():
        print(
            f"== Experiment {exp.id} has {len(exp.sizes)} instance size requests to test."
        )

        # Save the entire table just once
        path = os.path.join(args.data_dir, exp.id, today, args.tag)
        if not os.path.exists(path):
            os.makedirs(path)
        outfile = os.path.join(path, "results.json")

        # The set will be out of order
        sizes = list(exp.sizes)
        sizes.sort()

        # For each size batches:
        for size in sizes:
            # Reset times between experiments (we saved original times already)
            cli.times = {}

            # Now create the node groups!
            # This is N nodes for some unique set of instances from the original filtered set
            node_pool_name = f"node-pool-size-{size}"
            if node_pool_name in results["experiments"]:
                continue
            print(f"⭐️ Size {node_pool_name}")

            # Keep track if we had a failure
            failure_create = 0
            failure_delete = 0

            # This will wait for the cluster to be ready again. Also note I've seen it sometimes fail.
            # We should add a try / catch for that. We can ask for COMPACT or TIER_1 but most
            # instance types don't support it.
            try:
                cli.create_cluster_nodes(
                    node_pool_name,
                    node_count=size,
                    machine_type=exp.machine_type,
                    placement_policy="COMPACT",
                    threads_per_core=1,
                )
            except:
                print(f"Failure creating size {size}")
                failure_create += 1
                import IPython

                IPython.embed()

            # Now time deletion
            try:
                cli.delete_nodegroup(node_pool_name)
            except:
                print(f"Failure deleting size {size}")
                failure_delete += 1
                import IPython

                IPython.embed()

            # A new result object for each.
            result = {
                "times": copy.deepcopy(cli.times),
                "metadata": exp.export(),
                "machine_type": exp.machine_type,
                "id": node_pool_name,
                "size": size,
                "day": today,
                "tag": args.tag,
                "failure_create": failure_create,
                "failure_delete": failure_delete,
            }

            # Show and save results as we go
            results["experiments"][node_pool_name] = result
            print(json.dumps(results))
            write_json(results, outfile)

    print("Experiments are done!")

    # When we are done, delete the entire cluster
    # I hope this includes node groups, we will see
    cli.delete_cluster()
    results["delete_cluster"] = cli.times["delete_cluster"]
    results["end_time"] = str(datetime.now())
    print(json.dumps(results))
    write_json(results, outfile)


def main():
    """
    Run experiments for lammps, and collect hwloc info.
    """
    parser = get_parser()

    # If an error occurs while parsing the arguments, the interpreter will exit with value 2
    args, _ = parser.parse_known_args()
    if not os.path.exists(args.data_dir):
        os.makedirs(args.data_dir)
    if not args.tag:
        sys.exit("Please provide an experiment tag")

    # Only one of the zone or region is allowed
    if args.zone and args.region:
        sys.exit("You must select a single --zone OR --region.")
    location = args.zone or args.region

    # plan experiments!
    experiments = plan_experiments(args)
    print("🧪️ Experiments:")
    for exp in experiments:
        print(f"   {exp}")

    print("🪴️ Planning to run:")
    print(f"   Cluster name        : {args.cluster_name}")
    print(f"          tag          : {args.tag}")
    print(f"     location          : {location}")
    print(f"   Output Data         : {args.data_dir}")
    print(f"   Experiments         : {len(experiments)}")
    if not confirm_action("Would you like to continue?"):
        sys.exit("📺️ Cancelled!")

    # Main experiment running, show total time just for user FYI
    start_experiments = time.time()
    run_experiments(experiments, args)
    stop_experiments = time.time()
    total = stop_experiments - start_experiments
    print(f"total time to run is {total} seconds")


if __name__ == "__main__":
    main()
