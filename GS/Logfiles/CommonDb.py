import re
import sqlite3
from datetime import datetime
from sqlite3 import Connection, Error, Cursor


def get_date_from_string(line):
    return re.search(r"\d*-\d*-\d*", line).group().strip()


class TrainingDataRow:
    _timestamp: datetime = None
    _number_of_parallel_requests_start: int = None
    _number_of_parallel_requests_end: int = None
    _number_of_parallel_requests_finished: int = None
    _request_type: str = None
    _request_execution_time_ms: int = None

    @staticmethod
    def from_dict(object):
        row = TrainingDataRow()
        row.timestamp = object['time_stamp']
        row.number_of_parallel_requests_start = object['number_of_parallel_requests_start']
        row.number_of_parallel_requests_end = object['number_of_parallel_requests_end']
        row.number_of_parallel_requests_finished = object['number_of_parallel_requests_finished']
        row.request_type = object['request_type']
        row.request_execution_time_ms = object['response_time']

        return row

    def __str__(self):
        return str.strip(f"""
            timestamp: {self._timestamp},
            number_of_parallel_requests_start: {self._number_of_parallel_requests_start},
            number_of_parallel_requests_end: {self._number_of_parallel_requests_end},
            number_of_parallel_requests_finished: {self._number_of_parallel_requests_finished},
            request_type: {self._request_type},
            request_execution_time_ms: {self._request_execution_time_ms}
            """)

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def number_of_parallel_requests_start(self):
        return self._number_of_parallel_requests_start

    @property
    def number_of_parallel_requests_end(self):
        return self._number_of_parallel_requests_end

    @property
    def number_of_parallel_requests_finished(self):
        return self._number_of_parallel_requests_finished

    @property
    def request_type(self):
        return self._request_type

    @property
    def request_execution_time_ms(self):
        return self._request_execution_time_ms

    @timestamp.setter
    def timestamp(self, value: datetime):
        self._timestamp = value

    @number_of_parallel_requests_start.setter
    def number_of_parallel_requests_start(self, value):
        self._number_of_parallel_requests_start = value

    @number_of_parallel_requests_end.setter
    def number_of_parallel_requests_end(self, value):
        self._number_of_parallel_requests_end = value

    @number_of_parallel_requests_finished.setter
    def number_of_parallel_requests_finished(self, value):
        self._number_of_parallel_requests_finished = value

    @request_type.setter
    def request_type(self, value):
        self._request_type = value

    @request_execution_time_ms.setter
    def request_execution_time_ms(self, value):
        self._request_execution_time_ms = value


class SQLSelectExecutor:
    _sql_statement: str = ""

    def __init__(self, conn: Connection):
        self._conn = conn

    def set_custom_select_statement(self, sql: str):
        self._sql_statement = sql

    def construct_select_statement(self, table_name: str, columns: str = "*"):
        """
        :param table_name: table to select FROM
        :param columns: columns to SELECT or * for all
        :return:
        """

        statement = f"""SELECT {columns} FROM {table_name}"""

        self._sql_statement = statement

        return self

    def add_condition_to_statement(self, condition: str):
        self._sql_statement = f"{self._sql_statement} WHERE {condition}"

        return self

    def execute_select_statement(self) -> Cursor:
        """
        Execute the given SELECT statement
        :param conn: the Connection object
        :return: Cursor of the resultset
        """
        try:
            print("Executing %s" % self._sql_statement)
            cur = self._conn.cursor()
            cur.execute(self._sql_statement)

            return cur
        except Error as e:
            print("Error while executing %s" % self._sql_statement)
            print(e)

    def fetch_all_results(self, cursor: Cursor):
        def create_training_data_row(row_from_db: tuple) -> TrainingDataRow:
            row = TrainingDataRow()

            length = len(row_from_db)

            if length > 0:
                if isinstance(row_from_db[0], str):
                    row.timestamp = datetime.fromisoformat(row_from_db[0])
                else:
                    row.timestamp = row_from_db[0]
            if length > 1:
                row.number_of_parallel_requests_start = row_from_db[1]
            if length > 2:
                row.number_of_parallel_requests_end = row_from_db[2]
            if length > 3:
                row.number_of_parallel_requests_finished = row_from_db[3]
            if length > 4:
                row.request_type = row_from_db[4]
            if length > 5:
                row.request_execution_time_ms = row_from_db[5]

            return row

        data = map(create_training_data_row, cursor)

        return data


def create_connection(db_file) -> Connection:
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: path to database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def training_data_exists_in_db(db_connection: Connection, path_to_log_file: str) -> bool:
    file_timestamp = datetime.strptime(
        get_date_from_string(path_to_log_file),
        "%Y-%m-%d"
    )

    date_to_check = file_timestamp

    sql = f"""
    SELECT EXISTS (
        SELECT timestamp FROM gs_training_data
        WHERE strftime('%Y%m%d', timestamp) == "{date_to_check.strftime("%Y%m%d")}"
    );
    """

    select = SQLSelectExecutor(db_connection)
    select.set_custom_select_statement(sql)

    cur = select.execute_select_statement()
    results = list(select.fetch_all_results(cur))

    return results[0].timestamp == 1

