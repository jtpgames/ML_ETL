from os import path, mkdir
import sqlite3
from datetime import datetime
from glob import glob
from sqlite3 import Error, Connection, Cursor

from Common import get_date_from_string, read_data_line_from_log_file
from CommonDb import training_data_exists_in_db, TrainingDataRow, SQLSelectExecutor, create_connection


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
        `request_execution_time_ms` INT unsigned NOT NULL,
        PRIMARY KEY (`timestamp`)
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
        request_execution_time_ms
    )
    VALUES(
        "{row.timestamp}", 
        {row.number_of_parallel_requests_start},
        {row.number_of_parallel_requests_end},
        {row.number_of_parallel_requests_finished},
        "{row.request_type}",
        {row.request_execution_time_ms}
    );
    """

    return statement


def setup_db() -> Connection:
    pathToDb = r"db/trainingdata.db"

    if not path.exists("db"):
        mkdir("db")

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
            "idx_request_type",
            "request_type"
        ),
        True
    )

    return db_connection


if __name__ == '__main__':
    dbConnection = setup_db()

    for logFile in sorted(glob("../GS Logs Vision 21.12.20_03.12.21/Conv_2021-*.log")):
        if not training_data_exists_in_db(dbConnection, logFile):
            for line in read_data_line_from_log_file(logFile):
                execute_sql_statement(
                    dbConnection,
                    construct_insert_training_data_statement(
                        "gs_training_data",
                        TrainingDataRow.from_dict(line)
                    )
                )
            dbConnection.commit()
            print("Committed")
        else:
            print("Skipping ", logFile)

    dbConnection.close()
