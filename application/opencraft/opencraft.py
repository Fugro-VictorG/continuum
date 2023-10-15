import logging
import copy
import sys
import numpy as np
import pandas as pd

from application import application

def set_container_location(config):
    """Set registry location/path of containerized applications

    Args:
        config (dict): Parsed configuration
    """
    config["images"] = {
        "worker": "2000arp/opencraft_benchmark:opencraft_server",
        "endpoint": "2000arp/opencraft_benchmark:opencraft_bot",
    }

def add_options(_config):
    """Add config options for a particular module

    Args:
        config (ConfigParser): ConfigParser object

    Returns:
        list(list()): Options to add
    """
    settings = [
        ["steps_bot", int, lambda x: x >= 1, True, None],
        ["join_strategy", str, lambda x: x == "LinearJoin" or "FixedJoin" or "Default", True, "Default"]
    ]
    return settings

def verify_options(parser, config):
    """Verify the config from the module's requirements

    Args:
        parser (ArgumentParser): Argparse object
        config (ConfigParser): ConfigParser object
    """
    if config["benchmark"]["application"] != "opencraft":
        parser.error("ERROR: Application should be opencraft")
    elif "cache_worker" in config["benchmark"] and config["benchmark"]["cache_worker"] == "True":
        parser.error("ERROR: opencraft app does not support application caching")
    elif config["benchmark"]["resource_manager"] == "kubecontrol":
        parser.error("ERROR: Application opencraft does not support kubecontrol")
    elif config["infrastructure"]["edge_nodes"] >= 1 and config["infrastructure"]["cloud_nodes"] >= 1:
        parser.error("ERROR: Application opencraft does not support both cloud and edge nodes")

def start_worker(config, machines):
    """Set variables needed when launching the app on workers

    Args:
        config (dict): Parsed configuration
        machines (list(Machine object)): List of machine objects representing physical machines

    Returns:
        (dict): Application variables
        OR
        (list): Application variables
    """
    app_vars = {}
    return app_vars

def gather_worker_metrics(_machines, _config, worker_output, _starttime):
    """Calculates the average tick rate and its standard deviation per server.
    The server captures more than just the tick rate so this method can be adapted to get more metrics.
    Args:
        worker_output (list(list(str))): Output of each container that ran in cloud/edge.
        Element in outer list is the node, element in inner list is a line in the output.

        machines (list(Machine object)): List of machine objects representing physical machines
        config (dict): Parsed configuration
        starttime (datetime): Time that 'kubectl apply' is called to launche the benchmark

    Returns:
        list(dict): List of parsed output for each cloud or edge worker
    """
    worker_metrics = []
    if worker_output == []:
        return worker_metrics

    worker_set = {
        "worker_id": None,
        "ticks_mean": None,
        "ticks_stdev": None,
        "ticks_median": None
    }

    for i, out in enumerate(worker_output):
        logging.info("Parse output from worker node %i", i)
        w_metrics = copy.deepcopy(worker_set)
        w_metrics["worker_id"] = i

        # skip forward to header, which contains the labels of the columns (timestamp, key, value)
        # the first rows are only the logs of the server console which we don't care about :O

        idx = 0
        while "timestamp" not in out[idx]:
            idx += 1

        # filter data based on tick
        filtered_data = [out[idx].split()]
        for row_idx in range(idx + 1, len(out)):
            # change the following line to get more metrics captured
            row_split = out[row_idx].split()
            if "tick" == row_split[2]:
                filtered_data.append(row_split)

        # calculate metrics
        df = pd.DataFrame(filtered_data[1:], columns=filtered_data[0])
        df["value"] = pd.to_numeric(df["value"])
        # filter before the following lines if you have more than one metric
        w_metrics["ticks_mean"] = df["value"].mean()
        w_metrics["ticks_median"] = df["value"].median()
        w_metrics["ticks_stdev"] = df["value"].std()

        worker_metrics.append(w_metrics)

    return sorted(worker_metrics, key=lambda x: x["worker_id"])

def gather_endpoint_metrics(config, endpoint_output, container_names):
    if endpoint_output == []:
        return []

    for i, out in enumerate(endpoint_output):
        if i == 0:
            filtered_data = {
                "response_time_dig" : [],
                "response_time_place": []
                }
            e_metrics = {
                "response_time_dig_mean": None,
                "response_time_dig_median": None,
                "response_time_dig_stdev": None,
                "response_time_place_mean": None,
                "response_time_place_median": None,
                "response_time_place_stdev": None,
            }
            for line in out:
                line_partition = str(line).partition(": ")
                index_and_type = line_partition[0]
                latency = line_partition[2]
                if latency and ("Dig" in index_and_type):
                    filtered_data["response_time_dig"].append(int(latency))
                elif latency and ("Place" in index_and_type):
                    filtered_data["response_time_place"].append(int(latency))
            df = pd.DataFrame(filtered_data)
            e_metrics["response_time_dig_mean"] = df["response_time_dig"].mean()
            e_metrics["response_time_dig_median"] = df["response_time_dig"].median()
            e_metrics["response_time_dig_stdev"] = df["response_time_dig"].std()
            e_metrics["response_time_place_mean"] = df["response_time_place"].mean()
            e_metrics["response_time_place_median"] = df["response_time_place"].median()
            e_metrics["response_time_place_stdev"] = df["response_time_place"].std()

        logging.info("--------------ENDPOINT %s OUTPUT------------\n", i)
        for log in out:
            logging.info(log)
    return [e_metrics]

def format_output(config, worker_metrics, endpoint_metrics):
    """Format processed output to provide useful insights

    Args:
        config (dict): Parsed configuration
        worker_metrics (list(dict)): Metrics per worker node
        endpoint_metrics (list(dict)): Metrics per endpoint
    """
    logging.info("------------------------------------")
    logging.info("%s OUTPUT", config["mode"].upper())
    logging.info("------------------------------------")
    
    i = 0
    final_metrics = worker_metrics
    while i < len(final_metrics):
        final_metrics[i].update(endpoint_metrics[i])
        i += 1
    df = pd.DataFrame(final_metrics)
    df_no_indices = df.to_string(index=False)
    logging.info("\n%s", df_no_indices)
    
    # Print ouput in csv format
    logging.debug("Output in csv format\n%s", repr(df.to_csv()))