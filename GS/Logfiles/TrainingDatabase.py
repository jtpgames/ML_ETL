from datetime import datetime
from typing import Optional, Iterable

from sqlalchemy import create_engine, String, Float, TIMESTAMP, Engine, and_, func, select, insert, between
from sqlalchemy.dialects.mysql import SMALLINT, INTEGER
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped, Session

from StringUtils import get_date_from_string


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

    def __init__(self, entity: Optional[TrainingDataEntity] = None):
        if entity is not None:
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
    except Exception as e:
        print(e)

    return engine


def create_training_data_table(engine: Engine):
    try:
        # Create the table if it does not exist
        Base.metadata.create_all(engine, checkfirst=True)
    except Exception as e:
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


def read_all_training_data_from_db_using_sqlalchemy(db_path: str) -> Iterable[TrainingDataRow]:
    db_connection = create_connection_using_sqlalchemy(db_path, True)
    if db_connection is None:
        print("Could not read performance metrics")
        exit(1)

    with Session(db_connection) as session:
        stmt = select(TrainingDataEntity)

    for row in session.scalars(stmt):
        yield TrainingDataRow(row)


def read_training_data_from_db_between_using_sqlalchemy(db_path: str, begin: str, end: str) -> Iterable[TrainingDataRow]:
    db_connection = create_connection_using_sqlalchemy(db_path, True)
    if db_connection is None:
        print("Could not read performance metrics")
        exit(1)

    with Session(db_connection) as session:
        results = session.query(TrainingDataEntity).filter(between(
            func.strftime('%Y %m %d', TrainingDataEntity.timestamp),
            begin,
            end)
        ).all()

        for row in results:
            yield row
