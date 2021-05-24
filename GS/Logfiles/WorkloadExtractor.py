import argparse
import contextlib
import datetime
import glob
from os.path import join
from typing import Optional

from Common import dir_path, get_date_from_string, get_timestamp_from_line


class RequestsPerSecondTracker:
    def __init__(self, source_file_path: str):
        self._requests_counter = 0
        self._start: Optional[datetime] = None
        self._tracked_data = []

        from pathlib import Path
        name_of_log_file = Path(source_file_path).name
        target_path = Path(source_file_path) \
            .with_name("Requests_per_second_{}".format(get_date_from_string(name_of_log_file))) \
            .with_suffix(".log")

        print("Writing to ", target_path)
        self._target_file = open(target_path, mode="w")

    def process_log_line(self, line: str):
        if "CMD-START" not in line:
            return

        timestamp_of_request = get_timestamp_from_line(line)

        if self._start is None:
            self._start = timestamp_of_request

        difference = (timestamp_of_request - self._start)

        if difference.total_seconds() > 1:
            timestamp = self._start.strftime('%Y-%m-%d %H:%M:%S')

            self._tracked_data.append({"timestamp": self._start, "rps": self._requests_counter})
            self._target_file.write(f"{timestamp}\tRSP: {self._requests_counter}/s\n")
            self._requests_counter = 0
            self._start = timestamp_of_request

        self._requests_counter += 1

    def close(self):
        self._target_file.close()

        from pandas import DataFrame
        df = DataFrame.from_records(self._tracked_data)

        print(df)


@contextlib.contextmanager
def rps_tracker(path):
    theobj = RequestsPerSecondTracker(path)
    try:
        yield theobj
    finally:
        theobj.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert log files '
                                                 'of the GS legacy system '
                                                 'to our custom locust log file format.')
    parser.add_argument('--files', '-f',
                        type=str,
                        nargs='+',
                        help='the paths to the ARS log files')
    parser.add_argument('--directory', '-d',
                        type=dir_path,
                        help='the directory the log files are located in')

    args = parser.parse_args()

    if args.files is None and args.directory is None:
        parser.print_help()
        exit(1)

    logfilesToConvert = args.files if args.files is not None else []

    if args.directory is not None:
        logfiles = glob.glob(join(args.directory, "Merged_*.log"))
        logfiles.extend(glob.glob(join(args.directory, "teastore-cmd_*.log")))
        logfilesToConvert.extend(logfiles)

    # remove duplicates trick
    logfilesToConvert = sorted(set(logfilesToConvert))

    print("Logs to convert: " + str(logfilesToConvert))

    for path in logfilesToConvert:
        print("Reading from %s" % path)
        with open(path) as logfile, rps_tracker(path) as requests_per_second_tracker:
            counter = 0

            for line in logfile:
                counter = counter + 1
                if counter % 20000 == 0:
                    print("Processed {} entries".format(counter))

                requests_per_second_tracker.process_log_line(line)
