import re


def get_date_from_string(line):
    return re.search(r"\d*-\d*-\d*", line).group().strip()
