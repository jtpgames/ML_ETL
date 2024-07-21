import argparse
import glob
import json
import re
from datetime import datetime, time
from os.path import join
from typing import Tuple, TextIO

from rast_common.main.StringUtils import dir_path, get_timestamp_from_line
from rast_common.main.StringUtils import get_date_from_string


class NumberOfParallelCommandsTrackerEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, NumberOfParallelCommandsTracker):
            return {
                "requests_per_second": {str(k): v for k, v in obj.requests_per_second.items()},
                "requests_per_minute": {str(k): v for k, v in obj.requests_per_minute.items()},
            }
        return super().default(obj)


class NumberOfParallelCommandsTracker:
    def __init__(self):
        self.current_parallel_commands = 0
        self.requests_per_second: dict[time, int] = dict()
        self.requests_per_minute: dict[time, int] = dict()

    def process_log_line(self, line: str):
        if "CMD-START" in line:
            self.current_parallel_commands += 1
            timestamp = get_timestamp_from_line(line)
            time_of_request = timestamp.time()
            time_of_request = time_of_request.replace(time_of_request.hour, time_of_request.minute, time_of_request.second, 0)
            if time_of_request not in self.requests_per_second:
                self.requests_per_second[time_of_request] = 0
            self.requests_per_second[time_of_request] += 1
            time_of_request = time_of_request.replace(time_of_request.hour, time_of_request.minute, 0)
            if time_of_request not in self.requests_per_minute:
                self.requests_per_minute[time_of_request] = 0
            self.requests_per_minute[time_of_request] += 1
        elif "CMD-ENDE" in line:
            self.current_parallel_commands -= 1

    def get_requests_per_second_for(self, timestamp: datetime):
        time_of_request = timestamp.time()
        time_of_request = time_of_request.replace(time_of_request.hour, time_of_request.minute, time_of_request.second, 0)
        if time_of_request not in self.requests_per_second:
            return 0
        return self.requests_per_second[time_of_request]

    def get_requests_per_minute_for(self, timestamp: datetime):
        time_of_request = timestamp.time()
        time_of_request = time_of_request.replace(time_of_request.hour, time_of_request.minute, 0, 0)
        if time_of_request not in self.requests_per_minute:
            return 0
        return self.requests_per_minute[time_of_request]

    def reset(self):
        self.current_parallel_commands = 0
        self.requests_per_second: dict[time, int] = dict()
        self.requests_per_minute: dict[time, int] = dict()

    def to_json(self, file: TextIO):
        return json.dump(self, file, cls=NumberOfParallelCommandsTrackerEncoder, indent=2)


class RequestLogConverter:

    def __init__(self, args):
        self.started_commands = {}
        self.args = args

        self.parallel_commands_tracker = NumberOfParallelCommandsTracker()

    def read(self, path: str):

        from pathlib import Path
        name_of_log_file = Path(path).name
        target_path = Path(path) \
            .with_name("Conv_{}".format(get_date_from_string(name_of_log_file))) \
            .with_suffix(".log")

        print("Writing to ", target_path)
        target_file = open(target_path, mode="w")

        print("Reading from %s" % path)
        with open(path) as logfile:
            counter = 0

            for line in logfile:
                counter = counter + 1
                if counter % 20000 == 0:
                    print("Processed {} entries".format(counter))

                if "CMD-START" in line:
                    (tid, _) = self.process_threadid_and_timestamp(line)
                    self.process_cmd(line, tid)
                elif "CMD-ENDE" in line:
                    (tid, end_time) = RequestLogConverter.get_threadid_and_timestamp(line)

                    if tid not in self.started_commands:
                        print("Command ended without corresponding start log entry")
                        print("in file: ", logfile)
                        print("on line: ", line)
                        print(self.started_commands)
                        if not self.args.force:
                            input("Press ENTER to continue...")
                        continue

                    self.started_commands[tid][
                        "parallelCommandsEnd"] = self.parallel_commands_tracker.current_parallel_commands

                    start_time = self.started_commands[tid]["time"]

                    execution_time_ms = (end_time - start_time).total_seconds() * 1000

                    self.write_to_target_log(
                        {
                            "receivedAt": end_time,
                            "cmd": self.started_commands[tid]["cmd"],
                            "parallelRequestsStart": self.started_commands[tid]["parallelCommandsStart"],
                            "parallelRequestsEnd": self.started_commands[tid]["parallelCommandsEnd"],
                            "parallelCommandsFinished": self.started_commands[tid]["parallelCommandsFinished"],
                            "time": int(execution_time_ms)
                        },
                        target_file
                    )

                    # thread <tid> finished his command,
                    # increment counter of other commands
                    for cmd in self.started_commands.values():
                        cmd["parallelCommandsFinished"] = cmd["parallelCommandsFinished"] + 1

                    # ...remove from startedCommands
                    self.started_commands.pop(tid)

                self.parallel_commands_tracker.process_log_line(line)

        if len(self.started_commands) > 0:
            print("Commands remaining")
            print(self.started_commands)
            if not self.args.force:
                input("Press ENTER to continue...")
        self.started_commands.clear()

        target_path = Path(path) \
            .with_name("request_statistics_{}".format(get_date_from_string(name_of_log_file))) \
            .with_suffix(".json")

        with open(target_path, "w") as write_file:
            self.parallel_commands_tracker.to_json(write_file)

        self.parallel_commands_tracker.reset()
        target_file.close()

    @staticmethod
    def get_threadid_from_line(line: str) -> int:
        tid = re.search(r"\[\d*\]", line).group()
        tid = tid.replace("[", "", 1)
        tid = tid.replace("]", "", 1)
        tid = int(tid)

        return tid

    @staticmethod
    def get_threadid_from_line_optimized(line: str) -> int:
        found_left_bracket = False
        tid_string = []

        for c in line:
            if c == '[':
                found_left_bracket = True
                continue
            elif c == ']':
                break

            if found_left_bracket:
                tid_string.append(c)

        tid_string = ''.join(tid_string)
        tid = int(tid_string)

        return tid

    @staticmethod
    def write_to_target_log(data, target_file):
        receivedAt = data['receivedAt'].strftime('%Y-%m-%d %H:%M:%S,%f')

        firstPart = f"[{receivedAt:26}] " \
                    f"(PR: {data['parallelRequestsStart']:2}/" \
                    f"{data['parallelRequestsEnd']:2}/" \
                    f"{data['parallelCommandsFinished']:2})"
        secondPart = f"{data['cmd']:35}:"
        thirdPart = f"Response time {data['time']} ms"
        target_file.write(f"{firstPart} {secondPart} {thirdPart}\n")

    @staticmethod
    def write_ARS_CMDs_to_target_log(data, target_file):
        if "ID_REQ_KC_STORE7D3BPACKET" in data["cmd"]:
            RequestLogConverter.write_to_target_log(data, target_file)

    @staticmethod
    def get_threadid_and_timestamp(line: str) -> Tuple[int, datetime]:
        # format: [tid] yyyy-MM-dd hh-mm-ss.f

        tid = RequestLogConverter.get_threadid_from_line_optimized(line)

        timestamp = get_timestamp_from_line(line)

        return tid, timestamp

    def process_threadid_and_timestamp(self, line: str) -> Tuple[int, datetime]:

        tid, timestamp = RequestLogConverter.get_threadid_and_timestamp(line)

        if tid in self.started_commands.keys():
            print(tid, " already processes another command", self.started_commands[tid]["cmd"])
            print("new line ", line)
            if not self.args.force:
                input("Press ENTER to continue...")

        self.started_commands[tid] = {
            "time": timestamp,
            "cmd": None,
            "parallelCommandsStart": self.parallel_commands_tracker.current_parallel_commands,
            "parallelCommandsEnd": 0,
            "parallelCommandsFinished": 0
        }

        return tid, timestamp

    def process_cmd(self, line: str, lastTid: int):
        if "unbekanntes CMD" not in line:
            cmd = re.search(r"ID_\w+", line).group()
        else:
            cmd = "ID_Unknown"

        self.started_commands[lastTid]["cmd"] = cmd


def main():
    parser = argparse.ArgumentParser(description='Convert request log files '
                                                 '(in the format of the GS legacy system) '
                                                 'to our custom common log format.')
    parser.add_argument('--files', '-f',
                        type=str,
                        nargs='+',
                        help='the paths to the (merged) request log files')
    parser.add_argument('--directory', '-d',
                        type=dir_path,
                        help='the directory the log files are located in')
    parser.add_argument('--force',
                        action='store_true',
                        help='ignore errors in the log files')
    args = parser.parse_args()
    if args.files is None and args.directory is None:
        parser.print_help()
        exit(1)
    logfiles_to_convert = args.files if args.files is not None else []
    if args.directory is not None:
        logfiles = glob.glob(join(args.directory, "Merged_*.log"))
        logfiles.extend(glob.glob(join(args.directory, "teastore-cmd_*.log")))
        logfiles_to_convert.extend(logfiles)
    # remove duplicates trick
    logfiles_to_convert = sorted(set(logfiles_to_convert))
    print("Logs to convert: " + str(logfiles_to_convert))
    for path in logfiles_to_convert:
        converter = RequestLogConverter(args)
        print("Converting ", path)
        converter.read(path)


if __name__ == "__main__":
    main()
