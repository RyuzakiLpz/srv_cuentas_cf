import os
import logging
from datetime import datetime
from colorlog import ColoredFormatter
from loadProperties import leer_propiedades
from concurrent_log_handler import ConcurrentRotatingFileHandler
from src.resources.common.level import Nivel


def configure_logging():

    path_file_log = leer_propiedades('path.log')
    path_file_log = str(path_file_log)
    name_ms = 'srv_cuentas_cf'
    name_file_log = os.path.join(
        path_file_log, name_ms + "-" + current_date() + ".log")

    log_format = ColoredFormatter(
        '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S",
        reset=True,
        log_colors={
            'DEBUG': 'yellow',
            'INFO': 'green',
            'ERROR': 'red',
        }
    )
    logging.disable(logging.DEBUG)
    file_handler = ConcurrentRotatingFileHandler(
        name_file_log, maxBytes=20000000, backupCount=7)
    file_handler.setFormatter(log_format)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)

    logging.basicConfig(
        level=logging.DEBUG,
        encoding='UTF-8',
        handlers=[
            file_handler,
            console_handler,
        ]
    )


def current_date():
    now = datetime.now()
    date_time_formatter = now.strftime("%Y-%m-%d")
    return date_time_formatter


def log_trace(nivel: Nivel, mensaje):
    global total_time

    total_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]

    match nivel.name:
        case "INFORMATIVO":
            logging.info(mensaje, extra={"TiempoTotal": total_time, })
        case "DEBUG":
            logging.debug(mensaje, extra={"TiempoTotal": total_time})
        case "ERROR":
            logging.error(mensaje, extra={"TiempoTotal": total_time})
        case _:
            print('Nivel no mapeado')
    total_time = 0
