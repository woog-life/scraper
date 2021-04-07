import logging
from typing import Tuple, Optional

import requests
import os
import socket
import sys

import urllib3
from bs4 import BeautifulSoup

WOOG_TEMPERATURE_URL = os.getenv("WOOG_TEMPERATURE_URL") or "https://woog.iot.service.itrm.de/?accesstoken=LQ8MXn"
BACKEND_URL = os.getenv("BACKEND_URL") or "http://api.svc.cluster.local"
BACKEND_PATH = os.getenv("BACKEND_PATH") or "lake/{}/temperature"
WOOG_UUID = os.getenv("WOOG_UUID") or "69c8438b-5aef-442f-a70d-e0d783ea2b38"


def create_logger(name: str, level: int = logging.WARN) -> logging.Logger:
    logger = logging.Logger(name)
    ch = logging.StreamHandler(sys.stdout)

    formatting = "[{}] %(asctime)s\t%(levelname)s\t%(module)s.%(funcName)s#%(lineno)d | %(message)s".format(name)
    formatter = logging.Formatter(formatting)
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    logger.setLevel(level)

    return logger


def get_website() -> Tuple[str, bool]:
    logger = create_logger("get_webiste")
    url = WOOG_TEMPERATURE_URL

    logger.debug(f"Requesting {url}")
    response = requests.get(url)

    content = response.content.decode("utf-8")
    logger.debug(content)

    return content, True


def get_temperature_from_html(xml: str) -> Optional[float]:
    """
    Tries to parse the xml
    Throws ValueError if value is not of type float
    :param xml: xml returned from the woog iot api
    :return: temperature or `None` if not parsable/value is missing
    """
    logger = create_logger("get_temperature_from_html")

    soup = BeautifulSoup(xml, "xml")

    outer_tag = soup.find("Water_Temperature")
    logger.debug(f"outer_tag: {outer_tag}")
    if not outer_tag:
        logger.error(f"outer_tag not present in {xml}")
        return None

    value_tag = outer_tag.find("value")
    logger.debug(f"value_tag: {value_tag}")
    if not value_tag:
        logger.error(f"value_tag not present in {outer_tag}")
        return None

    logger.debug(f"text: {value_tag.text}")
    try:
        temperature = float(value_tag.text)
    except ValueError as e:
        logger.error(f"{value_tag.text} is not a float")
        raise e

    return temperature


def send_temperature_to_api(temperature: float) -> Tuple[Optional[requests.Response], str]:
    logger = create_logger("send_temperature_to_api")
    path = BACKEND_PATH.format(WOOG_UUID)
    url = "/".join([BACKEND_URL, path])

    logger.debug(f"Send {temperature} to {url}")

    try:
        response = requests.put(url, json={"temperature": temperature})
        logger.debug(f"success: {response.ok} | content: {response.content}")
    except (requests.exceptions.ConnectionError, socket.gaierror, urllib3.exceptions.MaxRetryError):
        logger.exception(f"Error while connecting to backend ({url})", exc_info=True)
        return None, url

    return response, url


def main():
    logger = create_logger("main")
    content, success = get_website()

    if not success:
        logger.error(f"Couldn't retrieve content from website: {content}")
        return

    try:
        temperature = get_temperature_from_html(content)
    except ValueError:
        logger.error("value_tag was not of type float")
        return

    response, generated_backend_url = send_temperature_to_api(temperature)

    if not response or not response.ok:
        logger.error(f"Failed to put temperature ({temperature}) to backend: {generated_backend_url}")
        sys.exit(1)


main()
