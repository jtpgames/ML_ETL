import asyncio
import glob
import json
import math
from datetime import datetime, time
from os import path, makedirs
from os.path import join
from pathlib import Path
from typing import Optional

import typer
from rast_common.main.StringUtils import get_date_from_string
from rast_common.main.SwitchAggFlowStats import SwitchAggFlowStats, SwitchAggFlowStatsDecoder
from rast_common.main.TrainingDatabase import create_connection_using_sqlalchemy, create_training_data_table, \
    training_data_exists_in_db_using_sqlalchemy, insert_training_data
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from Common import read_data_line_from_log_file
from rast_common.main.TrainingDatabase import TrainingDataRow
from RequestLogToCLF import NumberOfParallelCommandsTracker

import os


def setup_db_using_sqlalchemy(output_directory: str) -> Engine:
    current_dir = os.getcwd()

    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    db_directory = output_directory

    today = datetime.now().strftime("%Y-%m-%d")

    pathToDb = db_directory + "/trainingdata_{}.db".format(today)

    if not path.exists(db_directory):
        makedirs(db_directory)

    db_connection = create_connection_using_sqlalchemy(pathToDb, True)
    if db_connection is None:
        exit(1)

    create_training_data_table(db_connection)

    os.chdir(current_dir)

    return db_connection


def main(
        directory: str = typer.Argument(
            ...,
            help="The directory the log files are located in (relative to this scripts location)"
        ),
        output_directory: str = typer.Argument(
            r"../db",
            help="The directory the database should be saved at (relative to this scripts location)"
        ),
        query_netdata: bool = typer.Option(
            False,
            "--netdata", "-n",
            help="[WIP] Query a netdata instance for performance metrics"
        ),
        enrich_with_statistics: bool = typer.Option(
            True,
            "--enrich", "-e",
            help="Enrich training data with request and switch flow statistics, if available"
        )
):
    if query_netdata:
        from AcquirePerformanceMetricsFromNetdata import get_system_cpu_data, \
            get_row_from_dataframe_using_nearest_time

    db_connection = setup_db_using_sqlalchemy(output_directory)
    db_connection = Session(db_connection)

    loop = asyncio.get_event_loop()

    logfiles = glob.glob(join(directory, '**', 'Conv_*.log'), recursive=True)
    print("Logs to process: " + str(logfiles))

    for log_file in sorted(logfiles):
        if not training_data_exists_in_db_using_sqlalchemy(db_connection, log_file):

            print("Processing ", log_file)

            day_to_get_metrics_from = datetime.strptime(
                get_date_from_string(log_file),
                "%Y-%m-%d"
            )

            if query_netdata:
                resource_usage = loop.run_until_complete(
                    get_system_cpu_data(
                        loop,
                        day_to_get_metrics_from
                    )
                )

            tracker: Optional[NumberOfParallelCommandsTracker] = None
            flow_stats: Optional[list[SwitchAggFlowStats]] = None
            if enrich_with_statistics:
                print("Enriching with additional files")
                tracker = create_and_initialize_tracker(day_to_get_metrics_from, log_file)
                flow_stats = create_and_initialize_switchflowstats(day_to_get_metrics_from, log_file)

            training_data_rows: list[TrainingDataRow] = list()
            counter = 0
            for line in read_data_line_from_log_file(log_file):
                training_data_row = TrainingDataRow.from_logfile_entry(line)

                resource_usage_row = None
                if query_netdata:
                    # get resource usage from netdata
                    resource_usage_row = get_row_from_dataframe_using_nearest_time(
                        resource_usage,
                        training_data_row.timestamp.timestamp()
                    )
                if resource_usage_row is not None:
                    if math.isnan(resource_usage_row["total"]):
                        training_data_row.system_cpu_usage = 0
                    else:
                        training_data_row.system_cpu_usage = resource_usage_row["total"]
                else:
                    training_data_row.system_cpu_usage = 1

                if tracker is not None:
                    training_data_row.requests_per_second = tracker.get_requests_per_second_for(training_data_row.timestamp)
                    training_data_row.requests_per_minute = tracker.get_requests_per_minute_for(training_data_row.timestamp)

                if flow_stats is not None:
                    flow_stats_for_switch = flow_stats[0]
                    training_data_row.switch_id = flow_stats_for_switch.switch_id
                    training_data_row.bytes_per_second_transmitted_through_switch = flow_stats_for_switch \
                        .get_bytes_per_second_for(training_data_row.timestamp)
                    training_data_row.packets_per_second_transmitted_through_switch = flow_stats_for_switch \
                        .get_packets_per_second_for(training_data_row.timestamp)

                training_data_rows.append(training_data_row)

                counter = counter + 1
                if counter % 10000 == 0:
                    print("Processed {} entries".format(counter))

            insert_training_data(db_connection, training_data_rows)
            db_connection.commit()
            print("Committed")
        else:
            print("Skipping ", log_file)

    db_connection.close()


def create_and_initialize_tracker(day_to_get_metrics_from, log_file):
    target_path = Path(log_file) \
        .with_name("request_statistics_{}".format(day_to_get_metrics_from.date())) \
        .with_suffix(".json")

    if not target_path.exists():
        return None

    with open(target_path, "r") as f:
        request_statistics = json.load(f)
        tracker = NumberOfParallelCommandsTracker()
        tracker.requests_per_second = {time.fromisoformat(key): value for key, value in
                                       request_statistics['requests_per_second'].items()}
        tracker.requests_per_minute = {time.fromisoformat(key): value for key, value in
                                       request_statistics['requests_per_minute'].items()}
    return tracker


def create_and_initialize_switchflowstats(day_to_get_metrics_from, log_file) -> Optional[list[SwitchAggFlowStats]]:
    target_path = Path(log_file) \
        .with_name("switch_flow_stats_{}".format(day_to_get_metrics_from.date())) \
        .with_suffix(".json")

    if not target_path.exists():
        return None

    with open(target_path, "r") as f:
        all_flow_stats: list[SwitchAggFlowStats] = json.load(f, object_hook=SwitchAggFlowStatsDecoder.try_create_object)

        return all_flow_stats


if __name__ == "__main__":
    typer.run(main)
