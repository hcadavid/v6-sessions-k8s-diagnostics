"""
This file contains all partial algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the partial task
or directly to the user (if they requested partial results).
"""

import pandas as pd
from vantage6.algorithm.decorator import data, source_database
from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.decorator import algorithm_client, data
import time
from vantage6.algorithm.decorator.action import (
    data_extraction,
    pre_processing,
    federated,
    central
)
import argparse
import csv
import time
import requests
import psutil
import os
from typing import Any
import json
import socket
import platform
import dns.resolver

from vantage6.algorithm.tools.util import info, warn, error

def get_ip_addresses(family):
    for interface, snics in psutil.net_if_addrs().items():
        for snic in snics:
            if snic.family == family:
                yield (interface, snic.address)


def is_proxy_reachable(host: str, port: int):
    try:

        info(f"Checking if the FQDN of the node proxy ({host}:{str(port)}) can be resolved... ")

        ipaddr = socket.gethostbyname(host) 
        
        info(f"FQDN of the node proxy ({host}:{str(port)}) resolved as {ipaddr}... ")
        
        # Set timeout before creating connection
        socket.setdefaulttimeout(5)  # Set default timeout
        
        # Check if the port is listening
        sock = socket.create_connection((ipaddr, int(port)))
        
        info(f"Port {port} can be opened on the proxy ({host}) IP address: {ipaddr}")
        return True
    
    except socket.gaierror:
        info(f"Unreachable proxy: FQDN could not be resolved")
        return False
    except ConnectionRefusedError:
        info(f"Unreachable proxy: Connection refused on port {port}")
        return False
    except socket.timeout:
        info(f"Unreachable proxy: timeout occurred while trying to connecting to port {port}")
        return False
    except Exception as e:
        info(f"Unreachable proxy: Unexpected error: {str(e)}")
        return False
    
    finally:
        # Reset timeout after connection attempt
        socket.setdefaulttimeout(None)



def check_http_connection():
        try:
            # Attempt to reach www.google.com
            url = "http://www.google.com"
            timeout = 5
            
            # Send a GET request
            response = requests.get(url, timeout=timeout)
            
            # If the request was successful, return True
            return response.status_code == 200
        except requests.RequestException as e:
            print(f"HTTP connection failed: {e}")
            return False


def external_dns_reachable():
    try:
        # Attempt to connect to Google's DNS server
        dns_server = "8.8.8.8"
        port = 53
        
        # Create a UDP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        # Set timeout to avoid hanging indefinitely
        sock.settimeout(5)
        
        # Try to send a packet to the DNS server
        result = sock.connect_ex((dns_server, port))
        
        # Close the socket
        sock.close()
        
        # If connection was successful, return True
        if result == 0:
            print(f"Internet access detected on {platform.node()}")
            return True
        else:
            print(f"Internet not reachable on {platform.node()}")
            return False
    
    except socket.error as e:
        print(f"Connection error (can't determine Internet connection status): {e}")
        return False



@data_extraction
@source_database
def read_csv(database_uri: str) -> dict:
    return pd.read_csv(database_uri)


@pre_processing
@data(1)
def pre_process(df1: pd.DataFrame, column, dtype) -> pd.DataFrame:
    df1[column] = df1[column].astype(dtype)
    return df1


@pre_processing
@data(1)
def pre_process2(df1: pd.DataFrame, column, new_column) -> pd.DataFrame:
    df1[new_column] = df1[column] + 10
    return df1


@federated
@data(1)
def sum(df1: pd.DataFrame, column) -> dict:
    return {"sum": int(df1[column].sum())}


@federated
@data(1)
def echo(df1: pd.DataFrame, input) -> dict:    
    return {"echo": input}

@federated
@data(1)
def len(df1: pd.DataFrame, column) -> dict:    
    return {"len": int(df1[column].size),"data":5}


@federated
@data(1)
def fed_avg(df1: pd.DataFrame, column) -> dict:    
    numbers = df1[column]
    return {"len": int(numbers.size),"data":int(numbers.sum())}


@federated
@data(1)
def federated_avg(df1: pd.DataFrame, column) -> dict:
    # extract the column numbers from the CSV
    numbers = df1[column]

    # compute the sum, and count number of rows
    local_sum = numbers.sum()
    local_count = numbers.size

    print(f">>>>>>>>>localsum:{local_sum}, {type(local_sum)}")
    print(f">>>>>>>>>localcount:{local_count}, {type(local_count)}")

    # return the values as a dict
    return {
        "sum": int(local_sum),
        "count": int(local_count)
    }  



@federated
def network_status(sleep_time:int):

    ipv4s = list(get_ip_addresses(socket.AF_INET))
    ipv6s = list(get_ip_addresses(socket.AF_INET6))
    proxy_host = os.environ.get("HOST")
    proxy_port = os.environ.get("PORT")
    print(f"HOST env var: {proxy_host}")
    print(f"PORT env var: {proxy_port}")
    
    #host includes the protocol
    if proxy_host.startswith("http://") or proxy_host.startswith("https://"):
        proxy_host = proxy_host.split("://", 1)[1]


    print(f">>>>>Proxy FQDN {proxy_host} solved as {socket.gethostbyname(proxy_host)}")

    print(f'Host architecture:{platform.uname()[4]}')
    print("IPv4 Addresses:")
    for interface, ipv4 in ipv4s:
        print(f"{interface}: {ipv4}")

    print("\nIPv6 Addresses:")
    for interface, ipv6 in ipv6s:
        print(f"{interface}: {ipv6}")

    external_dns_enabled = external_dns_reachable()
    print(f'External DNS reachable (socket connection to port 53 test) :{"ENABLED" if external_dns_reachable else "DISABLED"}')    

    http_outbound_connection = check_http_connection()
    print(f'Internet access (http connection test) :{"ENABLED" if http_outbound_connection else "DISABLED"}')    

    proxy_rechable = is_proxy_reachable(proxy_host,proxy_port)
    print(f'V6-proxy status :{f"REACHABLE at {proxy_host}:{proxy_port}" if proxy_rechable else f"DISABLED or unreachable at {proxy_host}:{proxy_port}"}')    

    print(f'Waiting {sleep_time} seconds before finishing the job.')
    time.sleep(int(sleep_time))

    return {
        "proxy":f'{proxy_host}:{proxy_port}',
        "external_dns_reachable":external_dns_enabled,
        "http_connection_test_passed":http_outbound_connection,
        "proxy_reachable":proxy_rechable,
        "ipv4s_addresses":ipv4s,
        "ipv6s_addresses":ipv6s
    }


@central
@algorithm_client
def sleep(client: AlgorithmClient, sleep_time:int):
    print(f">>>> Sleeping for {sleep_time} seconds")
    time.sleep(sleep_time)


@central
@algorithm_client
def central_average(client: AlgorithmClient, column_name: str):

    # Info messages can help you when an algorithm crashes. These info
    # messages are stored in a log file which is send to the server when
    # either a task finished or crashes.
    print("Collecting participating organizations")
    

    # task = client.task.create(name="fedavg", description="subtask", collaboration=1,
    #                 organizations=[2,3,4],
    #                 databases=[{"label": "s2_dframe1", "type": "dataframe", "dataframe_id":"2"}], 
    #                 image="ghcr.io/hcadavid/v6-sessions-k8s-diagnostics:latest", method="federated_avg", 
    #                 action="federated compute",
    #                 input_={
    #                     "args": ["Age"],
    #                     "kwargs": {}
    #                 }
    # , session=2)

    task = client.task.create(name="central-fedavg", description="subtask",
                    organizations=[2,3,4],                    
                    input_={
                        "method": "federated_avg",
                        "args": ["Age"],
                        "kwargs": {}
                    })

    print(f">>>>{task}")

    #client.wait_for_results(task['job_id'])

    # Collect all organization that participate in this collaboration.
    # These organizations will receive the task to compute the partial.
    #organizations = client.organization.list()
    #ids = [organization.get("id") for organization in organizations]

    # Request all participating parties to compute their partial. This
    # will create a new task at the central server for them to pick up.
    # We've used a kwarg but is is also possible to use `args`. Although
    # we prefer kwargs as it is clearer.
    #info("Requesting partial computation")
    #task = client.task.create(
    #    input_={"method": "partial_average", "kwargs": {"column_name": column_name}},
    #    organizations=ids,
    #)

    # Now we need to wait until all organizations(/nodes) finished
    # their partial. We do this by polling the server for results. It is
    # also possible to subscribe to a websocket channel to get status
    # updates.
    # info("Waiting for results")
    # results = client.wait_for_results(task_id=task.get("id"))
    # info("Partial results are in!")

    # # Now we can combine the partials to a global average.
    # info("Computing global average")
    # global_sum = 0
    # global_count = 0
    # for output in results:
    #     global_sum += output["sum"]
    #     global_count += output["count"]

    return {"average": 0.5}
