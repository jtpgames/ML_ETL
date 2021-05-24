import argparse
import os
import re
from datetime import datetime
from re import search
from typing import Dict


def get_date_from_string(line):
    return search(r"\d*-\d*-\d*", line).group().strip()


def contains_timestamp_with_ms(line: str):
    return search(r"(?<=\])\s*\d*-\d*-\d*\s\d*:\d*:\d*\.\d*", line) is not None


def get_timestamp_from_string(line: str):
    return search(r"(?<=\])\s*\d*-\d*-\d*\s\d*:\d*:\d*\.?\d*", line).group().strip()


def get_timestamp_from_line(line: str) -> datetime:
    if contains_timestamp_with_ms(line):
        format_string = '%Y-%m-%d %H:%M:%S.%f'
    else:
        format_string = '%Y-%m-%d %H:%M:%S'

    return datetime.strptime(
        get_timestamp_from_string(line),
        format_string
    )


def dir_path(path):
    if os.path.isdir(path):
        return path
    else:
        raise argparse.ArgumentTypeError(f"readable_dir:{path} is not a valid path")


def readResponseTimesFromLogFile(path: str) -> Dict[datetime, float]:
    response_times = {}

    # if 'locust_log' not in path:
    #     return response_times

    with open(path) as logfile:
        for line in logfile:
            if 'Response time' not in line:
                continue

            time_stamp = datetime.strptime(search('\\[.*\\]', line).group(), '[%Y-%m-%d %H:%M:%S,%f]')
            response_time = search('(?<=Response time\\s)\\d*', line).group()

            response_times[time_stamp] = float(response_time) / 1000

    return response_times


def read_data_line_from_log_file(path: str):
    with open(path) as logfile:
        for line in logfile:
            if 'Response time' not in line:
                continue

            # Extract:
            # * Timestamp as DateTime
            # * Number of Parallel Requests
            # * Request Type
            # * Request Execution Time

            time_stamp = datetime.strptime(re.search('\\[.*\\]', line).group(), '[%Y-%m-%d %H:%M:%S,%f]')

            number_of_parallel_requests_s = re.search('(?<=PR:)(\\s*\\d*)/(\\s*\\d*)/(\\s*\\d*)', line).groups()
            number_of_parallel_requests_start = int(number_of_parallel_requests_s[0])
            number_of_parallel_requests_end = int(number_of_parallel_requests_s[1])
            number_of_parallel_requests_finished = int(number_of_parallel_requests_s[2])

            request_type = re.search(r"ID_\w+", line).group()

            response_time = re.search('(?<=Response time\\s)\\d*', line).group()

            yield {
                "time_stamp": time_stamp,
                "number_of_parallel_requests_start": number_of_parallel_requests_start,
                "number_of_parallel_requests_end": number_of_parallel_requests_end,
                "number_of_parallel_requests_finished": number_of_parallel_requests_finished,
                "request_type": request_type,
                "response_time": response_time
            }

