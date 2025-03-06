import logging
import os
from logging.handlers import RotatingFileHandler




def write_app_log(log_path):
    path = f"{log_path}/app"
    os.makedirs(path, exist_ok=True)
    
    # create handler
    handler = RotatingFileHandler(f"{path}/app.log", maxBytes=5 * 1024 * 1024, backupCount=10)

    # log configuration
    logger = logging.getLogger('file_logger')
    logger.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)
    return logger

def write_kafka_log(log_path):
    # check dir exists
    path = f"{log_path}/kafka"
    os.makedirs(path, exist_ok=True)

    # create handler
    handler = RotatingFileHandler(f"{path}/kafka.log", maxBytes=5 * 1024 * 1024, backupCount=10)

    # log configuration
    logger = logging.getLogger('kafka')
    logger.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)
    return logger

def write_debug_log(log_path):
    # check dir exists
    path = f"{log_path}/debug"
    os.makedirs(path, exist_ok=True)

    # create handler
    handler = RotatingFileHandler(f"{path}/debug.log", maxBytes=5 * 1024 * 1024, backupCount=10)

    # log configuration
    logger = logging.getLogger('debug')
    logger.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)
    return logger
