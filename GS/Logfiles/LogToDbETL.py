import asyncio
import math
from os import path, mkdir
import sqlite3
from datetime import datetime, timedelta
from glob import glob
from sqlite3 import Error, Connection, Cursor

import typer

from Common import get_date_from_string, read_data_line_from_log_file
from CommonDb import training_data_exists_in_db, TrainingDataRow, SQLSelectExecutor, create_connection, \
    read_all_performance_metrics_from_db


def execute_sql_statement(conn: Connection, statement_sql: str, print_statement: bool = False):
    """ execute the given sql statement
        :param conn: Connection object
        :param statement_sql: an SQL statement
        :param print_statement: print the sql statement to stdout before executing it
        :return:
        """
    try:
        if print_statement:
            print("Executing %s" % statement_sql)
        c = conn.cursor()
        c.execute(statement_sql)
    except Error as e:
        print("Error while executing %s" % statement_sql)
        print(e)


def construct_create_training_data_table_statement(table_name):
    statement = f""" 
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `timestamp` TIMESTAMP NOT NULL,
        `number_of_parallel_requests_start` SMALLINT unsigned NOT NULL,
        `number_of_parallel_requests_end` SMALLINT unsigned NOT NULL,
        `number_of_parallel_requests_finished` SMALLINT unsigned NOT NULL,
        `request_type` VARCHAR NOT NULL,
        `system_cpu_usage` FLOAT NOT NULL,
        `request_execution_time_ms` INT unsigned NOT NULL
    );
    """

    return statement


def construct_create_index_statement(table_name, index_name, column):
    return f"""
    CREATE INDEX IF NOT EXISTS `{index_name}` 
    ON {table_name}(`{column}`);
    """


def construct_insert_training_data_statement(table_name, row: TrainingDataRow):
    statement = f"""
    INSERT INTO {table_name}(
        timestamp, 
        number_of_parallel_requests_start, 
        number_of_parallel_requests_end,
        number_of_parallel_requests_finished,
        request_type,
        system_cpu_usage,
        request_execution_time_ms
    )
    VALUES(
        "{row.timestamp}", 
        {row.number_of_parallel_requests_start},
        {row.number_of_parallel_requests_end},
        {row.number_of_parallel_requests_finished},
        "{row.request_type}",
        {row.system_cpu_usage},
        {row.request_execution_time_ms}
    );
    """

    return statement


def setup_db() -> Connection:
    db_directory = r"../../db"

    today = datetime.now().strftime("%Y-%m-%d")

    pathToDb = db_directory + "/trainingdata_{}.db".format(today)
    # pathToDb = db_directory + "/trainingdata_cumulative.db"

    if not path.exists(db_directory):
        mkdir(db_directory)

    db_connection = create_connection(pathToDb)

    if db_connection is None:
        exit(1)

    execute_sql_statement(
        db_connection,
        construct_create_training_data_table_statement("gs_training_data"),
        True
    )

    execute_sql_statement(
        db_connection,
        construct_create_index_statement(
            "gs_training_data",
            "idx_timestamp",
            "timestamp"
        ),
        True
    )

    execute_sql_statement(
        db_connection,
        construct_create_index_statement(
            "gs_training_data",
            "idx_request_type",
            "request_type"
        ),
        True
    )

    return db_connection


def main(
        directory: str = typer.Argument(
            ...,
            help="The directory the log files are located in"
        ),
        query_netdata: bool = typer.Option(
            False,
            "--netdata", "-n",
            help="Query a netdata instance for performance metrics"
        )
):
    if query_netdata:
        from GS.AcquirePerformanceMetricsFromNetdata import get_system_cpu_data, \
            get_row_from_dataframe_using_nearest_time

    db_connection = setup_db()

    loop = asyncio.get_event_loop()

    for log_file in sorted(glob(f"{directory}/Conv_*.log")):
        if not training_data_exists_in_db(db_connection, log_file):

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

                execute_sql_statement(
                    db_connection,
                    construct_insert_training_data_statement(
                        "gs_training_data",
                        training_data_row
                    )
                )

                counter = counter + 1
                if counter % 10000 == 0:
                    print("Processed {} entries".format(counter))

            db_connection.commit()
            print("Committed")
        else:
            print("Skipping ", log_file)

    db_connection.close()


if __name__ == "__main__":
    typer.run(main)
