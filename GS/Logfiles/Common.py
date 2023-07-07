import re
from datetime import datetime


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
