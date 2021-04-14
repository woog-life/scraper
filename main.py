import inspect
import logging
import os
import socket
import sys
from datetime import datetime
from typing import Tuple, Optional, Callable, Union, NewType, List

import requests
import urllib3
from bs4 import BeautifulSoup, Tag
from telegram import Bot

WOOG_TEMPERATURE_URL = os.getenv("WOOG_TEMPERATURE_URL") or "https://woog.iot.service.itrm.de/?accesstoken=LQ8MXn"
# noinspection HttpUrlsUsage
# cluster internal communication
BACKEND_URL = os.getenv("BACKEND_URL") or "https://api.woog.life"
BACKEND_PATH = os.getenv("BACKEND_PATH") or "lake/{}/temperature"
WOOG_UUID = os.getenv("LARGE_WOOG_UUID")
API_KEY = os.getenv("API_KEY")

WATER_INFORMATION = NewType("WaterInformation", Tuple[str, float])
AIR_INFORMATION = NewType("AirInformation", Tuple[str, float])


def create_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    logger = logging.Logger(name)
    ch = logging.StreamHandler(sys.stdout)

    formatting = "[{}] %(asctime)s\t%(levelname)s\t%(module)s.%(funcName)s#%(lineno)d | %(message)s".format(name)
    formatter = logging.Formatter(formatting)
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    logger.setLevel(level)

    return logger


def send_telegram_alert(message: str, token: str, chatlist: List[str]):
    logger = create_logger(inspect.currentframe().f_code.co_name)
    if not token:
        logger.error("TOKEN not defined in environment, skip sending telegram message")
        return

    if not chatlist:
        logger.error("chatlist is empty (env var: TELEGRAM_CHATLIST)")

    for user in chatlist:
        Bot(token=token).send_message(chat_id=user, text=f"Error while executing: {message}")


def get_website() -> Tuple[str, bool]:
    logger = create_logger(inspect.currentframe().f_code.co_name)
    url = WOOG_TEMPERATURE_URL

    logger.debug(f"Requesting {url}")
    response = requests.get(url)

    content = response.content.decode("utf-8")
    logger.debug(content)

    return content, True


def parse_website_xml(xml: str) -> BeautifulSoup:
    return BeautifulSoup(xml, "xml")


def get_tag_text_from_xml(xml: Union[BeautifulSoup, Tag], name: str, conversion: Callable) -> Optional:
    tag = xml.find(name)

    if not tag:
        return None

    return conversion(tag.text)


def get_air_information(soup: BeautifulSoup) -> Optional[AIR_INFORMATION]:
    logger = create_logger(inspect.currentframe().f_code.co_name)

    air_temperature_tag = soup.find("Air_Temperature")
    logger.debug(f"air_temperature_tag: {air_temperature_tag}")
    if not air_temperature_tag:
        logger.error(f"air_Temperature not present in {soup}")
        return

    try:
        temperature = get_tag_text_from_xml(air_temperature_tag, "value", float)
    except ValueError:
        logger.error("value_tag was not of type float")
        return

    if temperature is None:
        logger.error(f"temperature was None in air_temperature value tag ({air_temperature_tag})")
        return

    try:
        iso_time: int = get_tag_text_from_xml(air_temperature_tag, "ts",
                                              lambda x: datetime.fromtimestamp(int(x) / 1000).isoformat())
    except ValueError:
        logger.exception("ts_tag is not valid", exc_info=True)
        return

    # noinspection PyTypeChecker
    # at this point pycharm doesn't think that the return type can be optional despite the many empty returns beforehand
    return iso_time, temperature


def get_water_information(soup: BeautifulSoup) -> Optional[WATER_INFORMATION]:
    logger = create_logger(inspect.currentframe().f_code.co_name)

    water_temperature_tag = soup.find("Water_Temperature")
    logger.debug(f"water_temperature_tag: {water_temperature_tag}")
    if not water_temperature_tag:
        logger.error(f"Water_Temperature not present in {soup}")
        return

    try:
        temperature = get_tag_text_from_xml(water_temperature_tag, "value", float)
    except ValueError:
        logger.error("value_tag was not of type float")
        return

    if temperature is None:
        logger.error(f"temperature was None in water_temperature value tag ({water_temperature_tag})")
        return

    try:
        iso_time: int = get_tag_text_from_xml(water_temperature_tag, "ts",
                                              lambda x: datetime.fromtimestamp(int(x) / 1000).isoformat())
    except ValueError:
        logger.exception("ts_tag is not valid", exc_info=True)
        return

    # noinspection PyTypeChecker
    # at this point pycharm doesn't think that the return type can be optional despite the many empty returns beforehand
    return iso_time, temperature


def send_data_to_backend(water_information: WATER_INFORMATION, air_information: AIR_INFORMATION) -> Tuple[Optional[requests.Response], str]:
    logger = create_logger(inspect.currentframe().f_code.co_name)
    path = BACKEND_PATH.format(WOOG_UUID)
    url = "/".join([BACKEND_URL, path])

    water_timestamp, water_temperature = water_information
    air_timestamp, air_temperature = air_information
    if water_temperature <= 0:
        return None, "water_temperature is <= 0, please approve this manually."

    headers = {"Authorization": f"Bearer {API_KEY}"}
    data = {"temperature": water_temperature, "time": water_timestamp}
    logger.debug(f"Send {data} to {url}")

    try:
        response = requests.put(url, json=data, headers=headers)
        logger.debug(f"success: {response.ok} | content: {response.content}")
    except (requests.exceptions.ConnectionError, socket.gaierror, urllib3.exceptions.MaxRetryError):
        logger.exception(f"Error while connecting to backend ({url})", exc_info=True)
        return None, url

    return response, url


def main() -> Tuple[bool, str]:
    logger = create_logger(inspect.currentframe().f_code.co_name)
    content, success = get_website()
    if not success:
        message = f"Couldn't retrieve website: {content}"
        logger.error(message)
        return False, message

    soup = parse_website_xml(content)

    water_information = get_water_information(soup)
    air_information = get_air_information(soup)

    if not water_information:
        message = f"Couldn't retrieve water information from {soup}"
        logger.error(message)
        return False, message

    if not air_information:
        message = f"Couldn't retrieve air information from {soup}"
        logger.error(message)
        return False, message

    response, generated_backend_url = send_data_to_backend(water_information, air_information)

    if not response or not response.ok:
        message = f"Failed to put data ({water_information}) to backend: {generated_backend_url}"
        logger.error(message)
        return False, message

    return True, ""


root_logger = create_logger("__main__")

if not WOOG_UUID:
    root_logger.error("LARGE_WOOG_UUID not defined in environment")
elif not API_KEY:
    root_logger.error("API_KEY not defined in environment")
else:
    success, message = main()
    if not success:
        root_logger.error(f"Something went wrong ({message})")
        token = os.getenv("TOKEN")
        chatlist = os.getenv("TELEGRAM_CHATLIST") or ""
        send_telegram_alert(message, token=token, chatlist=chatlist.split(","))
        sys.exit(1)
