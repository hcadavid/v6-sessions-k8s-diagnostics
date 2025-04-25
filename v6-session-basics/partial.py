"""
This file contains all partial algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the partial task
or directly to the user (if they requested partial results).
"""

import pandas as pd
from vantage6.algorithm.decorator import data, source_database
from vantage6.algorithm.decorator.action import (
    data_extraction,
    pre_processing,
    federated,
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

