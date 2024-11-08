from typing import Optional
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta, timezone
import pytz
from airflow.exceptions import AirflowException


def get_business_date(days, business_date=None, date_format="%Y%m%d"):
    """
    returns a string representing date

    :param days: the number of days added or removed
    :type days: int
    :param business_date:
    None to generate current date plus/subtract days,
    otherwise return business_date
    :type business_date: str
    :param date_format: output date format
    :type date_format: str
    """
    if business_date is None or business_date == "None":
        now = datetime.now() + timedelta(days=days)
        business_date = now.strftime(date_format)
        return business_date
    else:
        return business_date


def get_business_date_with_tz(
        days, business_date=None, date_format="%Y%m%d", input_tz="Etc/UTC"
):
    """
    returns a string representing date

    :param days: the number of days added or removed
    :type days: int
    :param business_date:
    None to generate current date plus/subtract days,
    otherwise return business_date
    :type business_date: str
    :param date_format: output date format
    :type date_format: str
    :param input_tz: timezone for business_date when None
                     show all timezones with: pytz.all_timezones
                     ex: Asia/Ho_Chi_Minh, UTC
    :type input_tz: str
    """
    if business_date is None or business_date == "None":
        tz = pytz.timezone(input_tz)
        now = datetime.now(tz) + timedelta(days=days)
        business_date = now.strftime(date_format)
        return business_date
    else:
        return business_date


def get_business_datetime_with_tz(
        business_datetime: Optional[str],
        input_format: str = "%Y%m%d-%H%M%S",
        output_format: str = "%Y%m%d-%H%M%S",
        input_tz: str = "Etc/UTC",
        output_tz: str = "Etc/UTC",
        days: int = 0,
        hours: int = 0,
) -> str:
    """
    Returns a string representing datetime with user format

    :param business_datetime:
    None to generate current date plus/subtract days, hours,
    otherwise return business_datetime
    :type business_datetime: str
    :param input_format: input datetime string format
    :type input_format: str
    :param output_format: output datetime string format
    :type output_format: str
    :param input_tz: input datetime time zone
    :type input_tz: str
    :param output_tz: output datetime time zone
    :type output_tz: str
    :param days: number of day to plus with input business_datetime
    :type days: int
    :param hours: number of hour to plus with input business_datetime
    :type hours: int
    """

    i_tz = pytz.timezone(input_tz)
    o_tz = pytz.timezone(output_tz)
    if business_datetime is None or business_datetime == "None":
        converted_datetime = datetime.now(o_tz)
    else:
        raw_datetime = datetime.strptime(business_datetime, input_format)
        converted_datetime = raw_datetime.replace(tzinfo=i_tz).astimezone(o_tz)

    converted_datetime = converted_datetime + timedelta(days=days, hours=hours)

    return converted_datetime.strftime(output_format)


def datetime_to_float(d):
    return d.timestamp()


def float_to_datetime(fl):
    return datetime.fromtimestamp(fl, tz=timezone.utc)


def get_partition_time(
        day_str, business_date_format="%Y%m%d", partition_date_format="%Y-%m-%d", add_days=0
):
    """
    returns a string representing date

    :param day_str: input date str
    :type day_str: str
    :param business_date_format: input date format
    :type business_date_format: str
    :param partition_date_format: output date format
    :type partition_date_format: str
    :param add_days: number of days added or removed
    :type add_days: int
    """
    now = datetime.strptime(day_str, business_date_format)
    if isinstance(add_days, int) and add_days != 0:
        now = now + timedelta(days=add_days)
    partition_time = now.strftime(partition_date_format)
    return partition_time


def get_modified_date(
        date_str, input_date_format="%Y%m%d", output_date_format="%Y%m%d", days=0, hours=-7
):
    """
    returns a string representing date

    :param date_str: input date str
    :type date_str: str
    :param input_date_format: input date format
    :type input_date_format: str
    :param output_date_format: output date format
    :type output_date_format: str
    :param days: number of days added or removed
    :type days: int
    :param hours: number of hours added or removed
    :type hours: int
    """
    date = datetime.strptime(date_str, input_date_format)
    modified_date = date + timedelta(days=days, hours=hours)
    return modified_date.strftime(output_date_format)


def get_timestamp_tz_from_datestr(
        date_str, date_format="%Y%m%d", days=0, hour=-7, tz=timezone.utc
):
    """
    returns a timestamp with timezone

    :param date_str: input date str
    :type date_str: str
    :param date_format: input date format
    :type date_format: str
    :param days: number of days added or removed
    :type days: int
    :param hour: number of hours added or removed
    :type hour: int
    :param tz: the timezone for result date (default UTC)
    :type tz: datetime.timezone
    """
    date = datetime.strptime(date_str, date_format).replace(tzinfo=tz)
    date_utc = date + timedelta(days=days, hours=hour)
    ts = date_utc.timestamp()
    return int(ts)


def str_2_date(date_str, date_format="%Y%m%d"):
    """
    returns a datetime type

    :param date_str: input date str
    :type date_str: str
    :param date_format: input date format
    :type date_format: str
    """
    return datetime.strptime(date_str, date_format)


def date_2_str(d, date_format="%Y%m%d"):
    """
    returns a string representing date

    :param d: input date
    :type d: datetime
    :param date_format: output date format
    :type date_format: str
    """
    return d.strftime(date_format)


def date_range(start_date, end_date, date_format="%Y-%m-%d"):
    """
    returns a range of date (str type)

    :param start_date: start date
    :type start_date: datetime
    :param end_date: end date
    :type end_date: datetime
    :param date_format: output date format
    :type date_format: str
    """
    result = []
    for n in range(int((end_date - start_date).days) + 1):
        d = start_date + timedelta(n)
        result.append(d.strftime(date_format))
    return result


def get_yesterday():
    return (datetime.now() - relativedelta(days=1)).strftime("%Y-%m-%d")


# it's just string of ObjectId, not actual ObjectId
def get_object_id_from_date(date_str, date_format="%Y-%m-%d"):
    dt = datetime.strptime(date_str, date_format)
    tmp = hex(int(dt.timestamp())) + "0000000000000000"
    return tmp[2:]


def get_date_from_object_id(object_id):
    return datetime.fromtimestamp(int(object_id[:8], 16))


def valid_date_time(date_str: str, date_format: str, date_delta: int = 0) -> str:
    if date_str is None or date_str == "None" or date_str == "":
        date_str = (datetime.now() + timedelta(days=date_delta)).strftime(date_format)

    try:
        correct_format = bool(datetime.strptime(date_str, date_format))
    except ValueError:
        correct_format = False
    if not correct_format:
        raise AirflowException(
            f"Incorrect data format, should be '{date_format}' but got {date_str}"
        )

    return date_str


def get_dates_between(start_date_str: str, end_date_str: str, format_string="%Y-%m-%d"):
    start_date = datetime.strptime(start_date_str, format_string).date()
    end_date = datetime.strptime(end_date_str, format_string).date()

    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime(format_string)
        date_list.append(date_str)
        current_date += timedelta(days=1)

    return date_list


def is_weekend(datetime: datetime):
    week_no = datetime.weekday()
    # 0: Monday, 6: Sunday
    return week_no == 6


def is_today_weekend():
    return is_weekend(datetime.today())


def is_today_afternoon_weekend():
    import pytz
    tm = datetime.now(pytz.timezone('Asia/Saigon'))
    hour_tm = tm.hour
    return is_weekend(tm) and hour_tm > 15
