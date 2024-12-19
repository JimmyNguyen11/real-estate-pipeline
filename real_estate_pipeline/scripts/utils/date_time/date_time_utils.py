from typing import Optional
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta, timezone
import pytz
from airflow.exceptions import AirflowException


def get_business_date(days, business_date=None, date_format="%Y%m%d"):
    """
    returns a string representing date
    """
    if business_date is None or business_date == "None":
        now = datetime.now() + timedelta(days=days)
        business_date = now.strftime(date_format)
        return business_date
    else:
        return business_date

def datetime_to_float(d):
    return d.timestamp()

def float_to_datetime(fl):
    return datetime.fromtimestamp(fl, tz=timezone.utc)

def str_2_date(date_str, date_format="%Y%m%d"):
    """
    returns a datetime type
    """
    return datetime.strptime(date_str, date_format)


def date_2_str(d, date_format="%Y%m%d"):
    """
    returns a string representing date
    """
    return d.strftime(date_format)


def date_range(start_date, end_date, date_format="%Y-%m-%d"):
    """
    returns a range of date (str type)
    """
    result = []
    for n in range(int((end_date - start_date).days) + 1):
        d = start_date + timedelta(n)
        result.append(d.strftime(date_format))
    return result


def get_yesterday():
    return (datetime.now() - relativedelta(days=1)).strftime("%Y-%m-%d")