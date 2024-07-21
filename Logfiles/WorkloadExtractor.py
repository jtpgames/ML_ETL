import argparse
import contextlib
import datetime
import glob
import re
from os.path import join
from typing import Optional

from rast_common.main.StringUtils import dir_path, get_timestamp_from_line
from rast_common.main.StringUtils import get_date_from_string


class RequestFilter:
    def __init__(self, source_file_path: str, request_type: str):
        """
        :param source_file_path: The path to a command log file.
        :param request_type: The type of request to include in the output. All other request types are omitted.
        """

        from pathlib import Path
        target_path = Path(source_file_path) \
            .with_name("Request_Statistics") \
            .with_suffix(".log")

        print("Writing to ", target_path)
        self._target_file = open(target_path, mode="w")

        self._request_type = request_type

    def process_log_line(self, line: str):
        s = re.search(r"ID_\w+", line)
        if s is None:
            return

        cmd = s.group()

        if cmd == self._request_type:
            self._target_file.write(f"{line}\n")


class RequestNamesTracker:
    def __init__(self, source_file_path: str):
        self._known_request_names = set()

        from pathlib import Path
        target_path = Path(source_file_path) \
            .with_name("Request_Names") \
            .with_suffix(".log")

        print("Writing to ", target_path)
        self._target_file = open(target_path, mode="w")

    def process_log_line(self, line: str):
        if "CMD-START" not in line:
            return

        s = re.search(r"ID_\w+", line)
        if s is None:
            return

        cmd = s.group()

        if cmd not in self._known_request_names:
            self._known_request_names.add(cmd)

            self._target_file.write(f"{cmd}\n")

    def close(self):
        self._target_file.close()


class RequestsPerSecondTracker:
    def __init__(self, source_file_path: str):
        self._requests_per_second_counter = 0
        self._requests_per_second_start_time: Optional[datetime] = None

        self._requests_per_hour_counter = 0
        self._requests_per_hour_start_time: Optional[datetime] = None

        self._tracked_data = []
        self._total_amount_of_requests = 0

        # GS-specific: Ignore the requests that are send to the ARS by the alarm devices
        self.IGNORE_REQUESTS = {'ID_REQ_KC_STORE7D3BPACKET'}

        from pathlib import Path
        name_of_log_file = Path(source_file_path).name
        target_path = Path(source_file_path) \
            .with_name("Requests_per_time_unit_{}".format(get_date_from_string(name_of_log_file))) \
            .with_suffix(".log")

        print("Writing to ", target_path)
        self._target_file = open(target_path, mode="w")

    def process_log_line(self, line: str):
        if "CMD-START" not in line:
            return

        for request_to_ignore in self.IGNORE_REQUESTS:
            if request_to_ignore in line:
                return

        self._total_amount_of_requests += 1

        timestamp_of_request = get_timestamp_from_line(line)

        if self._requests_per_second_start_time is None:
            self._requests_per_second_start_time = timestamp_of_request

        if self._requests_per_hour_start_time is None:
            self._requests_per_hour_start_time = timestamp_of_request

        difference = (timestamp_of_request - self._requests_per_second_start_time)
        if difference.total_seconds() > 1:
            self._write_requests_per_second_into_log(timestamp_of_request)

        difference = (timestamp_of_request - self._requests_per_hour_start_time)
        if difference.total_seconds() > 3600:
            self._write_requests_per_hour_into_log(timestamp_of_request)

        self._requests_per_second_counter += 1
        self._requests_per_hour_counter += 1

    def _write_requests_per_second_into_log(self, timestamp_of_last_request):
        timestamp = self._requests_per_second_start_time.strftime('%Y-%m-%d %H:%M:%S')

        self._tracked_data.append(
            {"timestamp": self._requests_per_second_start_time, "rps": self._requests_per_second_counter})
        self._target_file.write(f"{timestamp}\tRPS: {self._requests_per_second_counter}/s\n")
        self._requests_per_second_counter = 0
        self._requests_per_second_start_time = timestamp_of_last_request

    def _write_requests_per_hour_into_log(self, timestamp_of_last_request):
        timestamp = self._requests_per_hour_start_time.strftime('%Y-%m-%d %H:%M:%S')

        self._tracked_data.append(
            {"timestamp": self._requests_per_hour_start_time, "rph": self._requests_per_hour_counter})
        self._target_file.write(f"{timestamp}\tRPH: {self._requests_per_hour_counter}/h\n")
        self._requests_per_hour_counter = 0
        self._requests_per_hour_start_time = timestamp_of_last_request

    def close(self):
        self._write_requests_per_second_into_log(None)
        self._write_requests_per_hour_into_log(None)

        self._target_file.write(f"Total count: {self._total_amount_of_requests}\n")

        self._target_file.close()

        # from pandas import DataFrame
        # df = DataFrame.from_records(self._tracked_data)
        #
        # print(df)


@contextlib.contextmanager
def rps_tracker(path):
    theobj = RequestsPerSecondTracker(path)
    try:
        yield theobj
    finally:
        theobj.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extracts the workload of a system from its command log files '
                                                 'and writes the workload to a series of files for '
                                                 'later processing.')
    parser.add_argument('--files', '-f',
                        type=str,
                        nargs='+',
                        help='the paths to the log files')
    parser.add_argument('--directory', '-d',
                        type=dir_path,
                        help='the directory the log files are located in')

    args = parser.parse_args()

    if args.files is None and args.directory is None:
        parser.print_help()
        exit(1)

    logfilesToConvert = args.files if args.files is not None else []

    if args.directory is not None:
        logfiles = glob.glob(join(args.directory, "*-Merged_*.log"))
        logfiles.extend(glob.glob(join(args.directory, "teastore-cmd_*.log")))
        logfilesToConvert.extend(logfiles)

    # remove duplicates trick
    logfilesToConvert = sorted(set(logfilesToConvert))

    print("Logs to convert: " + str(logfilesToConvert))

    request_names_tracker = RequestNamesTracker(logfilesToConvert[0])

    request_statistics = RequestFilter(logfilesToConvert[0], 'ID_REQ_KC_STORE7D3BPACKET')

    line_counter = 0

    for path in logfilesToConvert:
        print("Reading from %s" % path)
        with open(path) as logfile, rps_tracker(path) as requests_per_second_tracker:
            counter = 0

            for line in logfile:
                counter = counter + 1
                if counter % 20000 == 0:
                    print("Processed {} entries".format(counter))

                requests_per_second_tracker.process_log_line(line)
                request_names_tracker.process_log_line(line)
                request_statistics.process_log_line(line)

            line_counter += counter

    request_names_tracker.close()

    print(f"Total lines processed: {line_counter}")
