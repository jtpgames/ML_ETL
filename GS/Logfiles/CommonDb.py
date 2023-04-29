import sqlite3
from datetime import datetime
from sqlite3 import Connection, Error, Cursor
from typing import Optional, Iterable

from sqlalchemy import create_engine, String, Float, TIMESTAMP, Engine, and_, func, select, insert
from sqlalchemy.dialects.mysql import SMALLINT, INTEGER
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped, Session

from pandas import DataFrame

# from GS.Logfiles.Common import get_date_from_string
from Common import get_date_from_string


class Base(DeclarativeBase):
    pass


class TrainingDataEntity(Base):
    __tablename__ = 'training_data'
    id: Mapped[int] = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, index=True)
    number_of_parallel_requests_start: Mapped[int] = mapped_column(SMALLINT(unsigned=True), nullable=False)
    number_of_parallel_requests_end: Mapped[int] = mapped_column(SMALLINT(unsigned=True), nullable=False)
    number_of_parallel_requests_finished: Mapped[int] = mapped_column(SMALLINT(unsigned=True), nullable=False)
    request_type: Mapped[str] = mapped_column(String, nullable=False, index=True)
    system_cpu_usage: Mapped[float] = mapped_column(Float, nullable=False)
    requests_per_second: Mapped[int] = mapped_column(INTEGER(unsigned=True), nullable=False)
    requests_per_minute: Mapped[int] = mapped_column(INTEGER(unsigned=True), nullable=False)
    request_execution_time_ms: Mapped[int] = mapped_column(INTEGER(unsigned=True), nullable=False)


class TrainingDataRow:
    _timestamp: datetime = None
    _number_of_parallel_requests_start: int = None
    _number_of_parallel_requests_end: int = None
    _number_of_parallel_requests_finished: int = None
    _request_type: str = None
    _system_cpu_usage: float = 0.
    _requests_per_second: int = 0
    _requests_per_minute: int = 0
    _request_execution_time_ms: int = None

    @staticmethod
    def from_logfile_entry(logfile_entry):
        row = TrainingDataRow()
        row.timestamp = logfile_entry['time_stamp']
        row.number_of_parallel_requests_start = logfile_entry['number_of_parallel_requests_start']
        row.number_of_parallel_requests_end = logfile_entry['number_of_parallel_requests_end']
        row.number_of_parallel_requests_finished = logfile_entry['number_of_parallel_requests_finished']
        row.request_type = logfile_entry['request_type']
        row.request_execution_time_ms = logfile_entry['response_time']

        return row

    def __init__(self):
        pass

    def __init__(self, entity: TrainingDataEntity):
        self._timestamp = entity.timestamp
        self._number_of_parallel_requests_start = entity.number_of_parallel_requests_start
        self._number_of_parallel_requests_end = entity.number_of_parallel_requests_end
        self._number_of_parallel_requests_finished = entity.number_of_parallel_requests_finished
        self._request_type = entity.request_type
        self._system_cpu_usage = entity.system_cpu_usage
        self._requests_per_second = entity.requests_per_second
        self._requests_per_minute = entity.requests_per_minute
        self._request_execution_time_ms = entity.request_execution_time_ms

    def __str__(self):
        return str.strip(f"""
            timestamp: {self._timestamp},
            number_of_parallel_requests_start: {self._number_of_parallel_requests_start},
            number_of_parallel_requests_end: {self._number_of_parallel_requests_end},
            number_of_parallel_requests_finished: {self._number_of_parallel_requests_finished},
            request_type: {self._request_type},
            system_cpu_usage: {self._system_cpu_usage},
            requests_per_second: {self._requests_per_second},
            requests_per_minute: {self._requests_per_minute},
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
    def system_cpu_usage(self):
        return self._system_cpu_usage

    @property
    def requests_per_second(self):
        return self._requests_per_second

    @property
    def requests_per_minute(self):
        return self._requests_per_minute

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

    @system_cpu_usage.setter
    def system_cpu_usage(self, value):
        self._system_cpu_usage = value

    @requests_per_second.setter
    def requests_per_second(self, value):
        self._requests_per_second = value

    @requests_per_minute.setter
    def requests_per_minute(self, value):
        self._requests_per_minute = value

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
                row.system_cpu_usage = row_from_db[5]
            if length > 6:
                row.request_execution_time_ms = row_from_db[6]

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


def create_connection_using_sqlalchemy(db_file, enable_sql_logging=False) -> Optional[Engine]:
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: path to database file
    :param enable_sql_logging: enable logging of SQL statements
    :return: Engine object or None
    """
    engine = None
    try:
        engine = create_engine(f'sqlite:///{db_file}', echo=enable_sql_logging)
    except Error as e:
        print(e)

    return engine


def create_training_data_table(engine: Engine):
    try:
        # Create the table if it does not exist
        Base.metadata.create_all(engine, checkfirst=True)
    except Error as e:
        print(e)


def training_data_exists_in_db_using_sqlalchemy(session: Session, path_to_log_file: str) -> bool:
    file_timestamp = datetime.strptime(
        get_date_from_string(path_to_log_file),
        "%Y-%m-%d"
    )

    date_to_check = file_timestamp

    exists_query = session.query(
        select(TrainingDataEntity.timestamp)
        .where(and_(func.strftime('%Y%m%d', TrainingDataEntity.timestamp) == date_to_check.strftime("%Y%m%d")))
        .exists()
    )
    exists_result = session.execute(exists_query).scalar()

    return exists_result


def insert_training_data(session: Session, rows: list):
    session.execute(insert(TrainingDataEntity), [
        {
            "timestamp": row.timestamp,
            "number_of_parallel_requests_start": row.number_of_parallel_requests_start,
            "number_of_parallel_requests_end": row.number_of_parallel_requests_end,
            "number_of_parallel_requests_finished": row.number_of_parallel_requests_finished,
            "request_type": row.request_type,
            "system_cpu_usage": row.system_cpu_usage,
            "request_execution_time_ms": row.request_execution_time_ms,
            "requests_per_second": row.requests_per_second,
            "requests_per_minute": row.requests_per_minute
        }
        for row in rows
    ])


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


def read_all_training_data_from_db(db_path: str) -> Iterable[TrainingDataRow]:
    db_connection = create_connection(db_path)

    if db_connection is None:
        print("Could not read performance metrics")
        exit(1)

    select = SQLSelectExecutor(db_connection) \
        .construct_select_statement("gs_training_data")

    cur = select.execute_select_statement()

    for row in select.fetch_all_results(cur):
        yield row

    db_connection.close()


def read_all_training_data_from_db_using_sqlalchemy(db_path: str) -> Iterable[TrainingDataRow]:
    db_connection = create_connection_using_sqlalchemy(db_path, True)
    if db_connection is None:
        print("Could not read performance metrics")
        exit(1)

    with Session(db_connection) as session:
        stmt = select(TrainingDataEntity)

    for row in session.scalars(stmt):
        yield TrainingDataRow(row)


known_request_types = {}


def read_all_performance_metrics_from_db(db_path: str):
    response_times = []

    begin = datetime.now()

    for row in read_all_training_data_from_db_using_sqlalchemy(db_path):
        time_stamp = row.timestamp

        weekday = time_stamp.weekday()

        # we are only interested in the time of day, not the date
        time = time_stamp.timetz()
        milliseconds = time.microsecond / 1000000
        time_of_day_in_seconds = milliseconds + time.second + time.minute * 60 + time.hour * 3600

        # time_of_request = time_stamp.timestamp()

        time_of_request = time_of_day_in_seconds

        request_type = row.request_type

        if request_type not in known_request_types:
            known_request_types[request_type] = len(known_request_types)

        request_type_as_int = known_request_types[request_type]

        response_times.append((
            time_of_request,
            weekday,
            row.number_of_parallel_requests_start,
            row.number_of_parallel_requests_end,
            row.number_of_parallel_requests_finished,
            request_type_as_int,
            float(row.system_cpu_usage),
            float(row.request_execution_time_ms) / 1000,
        ))

    df = DataFrame.from_records(
        response_times,
        columns=[
            'Timestamp',
            'WeekDay',
            'PR 1',
            'PR 2',
            'PR 3',
            'Request Type',
            'CPU (System)',
            'Response Time s'
        ]
    )

    print(f"read_all_performance_metrics_from_db finished in {(datetime.now() - begin).total_seconds()} s")

    # print("== " + path + "==")
    # print(df.describe())
    # print("Number of response time outliers: %i" % len(detect_response_time_outliers(df)))

    return df
