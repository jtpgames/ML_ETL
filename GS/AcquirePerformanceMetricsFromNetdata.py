import asyncio
import json
import logging
from asyncio import AbstractEventLoop
from datetime import datetime, timedelta
from time import strftime, localtime

import aiohttp
import async_timeout
from netdata import Netdata
from pandas import DataFrame

_logger = logging.getLogger(__name__)


async def get_system_cpu_data(loop: AbstractEventLoop, day_to_get_metrics_from: datetime):
    """Get the data from a Netdata instance."""
    async with aiohttp.ClientSession() as session:
        data = Netdata("fra01.helarion.eu", loop, session, port=19999)
        # # Get data for the CPU
        # await data.get_data("system.cpu")
        # print(json.dumps(data.values, indent=4, sort_keys=True))

        # # Print the current value of the system's CPU
        # _logger.info("CPU System: %s", round(data.values["system"], 2))

        # Get system cpu usage
        dataframe = await get_data_from_netdata_async(
            data,
            loop,
            session,
            day_to_get_metrics_from,
            dimension="system"
        )
        print_dataframe(dataframe)

        return dataframe

        # Get data for the cpu user group and the system.cpu dimension
        # chart = "groups.cpu_user"
        # dimension = "mysql"

        # dataframe = await get_data_from_netdata_async(data, session, day_to_get_metrics_from, chart, dimension)
        # print_dataframe(dataframe)


def print_dataframe(dataframe: DataFrame):
    def format_date(x, pos=None):
        unix_timestamp = x

        return strftime("%d.%m %H:%M:%S", localtime(unix_timestamp))

    dataframe["time"] = dataframe["time"].apply(format_date)
    print(dataframe)


async def get_data_from_netdata_async(
        netdata: Netdata,
        loop: AbstractEventLoop,
        session: aiohttp.ClientSession,
        date_to_retrieve: datetime,
        chart: str = "system.cpu",
        dimension: str = ""
) -> DataFrame:
    """
    Retrieve performance metrics from netdata using the data endpoint.
    :param netdata: Existing netdata instance
    :param loop: Existing AbstractEventLoop instance
    :param session: Existing ClientSession instance
    :param date_to_retrieve: the date of the day to retrieve data from
    :param chart: Chart to get data from, defaults to system.cpu
    :param dimension: 'Column' of the returned chart, defaults to
    """

    data_endpoint = "data?chart={chart}&dimensions={dimension}&before={end}&after={start}&options=seconds"

    day_to_get_metrics_from = date_to_retrieve
    start_of_the_day = datetime(day_to_get_metrics_from.year, day_to_get_metrics_from.month,
                                day_to_get_metrics_from.day)
    end_of_the_day = datetime(day_to_get_metrics_from.year, day_to_get_metrics_from.month,
                              day_to_get_metrics_from.day, 23, 59, 59)

    _logger.debug(start_of_the_day)
    _logger.debug(end_of_the_day)

    url = "{}{}".format(netdata.base_url,
                        data_endpoint.format(chart=chart,
                                             dimension=dimension,
                                             end=int(end_of_the_day.timestamp()),
                                             start=int(start_of_the_day.timestamp())
                                             )
                        )

    _logger.debug(url)

    with async_timeout.timeout(5, loop=loop):
        response = await session.get(url)

    json_data = await response.json()

    dataframe = DataFrame(json_data["data"], columns=json_data["labels"])

    return dataframe


def get_row_from_dataframe_using_nearest_time(dataframe: DataFrame, timestamp: float) -> DataFrame:
    # we use rounding to get the nearest integer
    # if x is th number of seconds of our timestamp
    # up to x.499 we get x and after x.500 we get x+1 sec

    nearest_time = round(timestamp)

    _logger.debug(nearest_time)

    return dataframe.query('time == @nearest_time')


if __name__ == '__main__':
    # configure root logger

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    root_logger.addHandler(ch)

    loop = asyncio.get_event_loop()

    # Get data from yesterday
    day_to_get_metrics_from = datetime.now()  # - timedelta(days=1)

    loop.run_until_complete(get_system_cpu_data(loop, day_to_get_metrics_from))
