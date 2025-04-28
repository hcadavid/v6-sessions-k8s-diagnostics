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
from vantage6.algorithm.decorator.action import (
    data_extraction,
    pre_processing,
    federated,
    central
)


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

    client.wait_for_results(task['job_id'])

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
