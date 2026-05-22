import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import sys

from waitress import serve

from app import app


def _configure_logging():
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

    log_dir = Path(os.getenv('YEMAT_LOG_DIR', Path(__file__).resolve().parent / 'logs'))
    log_dir.mkdir(parents=True, exist_ok=True)
    server_log_path = log_dir / 'waitress.log'
    access_log_path = log_dir / 'access.log'
    enable_console = str(os.getenv('YEMAT_LOG_TO_CONSOLE', '1')).strip().lower() in {'1', 'true', 'yes', 'on'}

    logger = logging.getLogger('yemat.waitress')
    access_logger = logging.getLogger('yemat.access')
    if logger.handlers and access_logger.handlers:
        return logger, access_logger

    logger.setLevel(logging.INFO)
    access_logger.setLevel(logging.INFO)

    server_handler = TimedRotatingFileHandler(
        server_log_path,
        when='midnight',
        interval=1,
        backupCount=14,
        encoding='utf-8-sig',
    )
    server_handler.suffix = '%Y-%m-%d'
    server_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(server_handler)

    access_handler = TimedRotatingFileHandler(
        access_log_path,
        when='midnight',
        interval=1,
        backupCount=14,
        encoding='utf-8-sig',
    )
    access_handler.suffix = '%Y-%m-%d'
    access_handler.setFormatter(logging.Formatter('%(asctime)s [ACCESS] %(message)s'))
    access_logger.addHandler(access_handler)

    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(console_handler)

        access_console_handler = logging.StreamHandler(sys.stdout)
        access_console_handler.setFormatter(logging.Formatter('%(asctime)s [ACCESS] %(message)s'))
        access_logger.addHandler(access_console_handler)

    logger.propagate = False
    access_logger.propagate = False
    return logger, access_logger


def main():
    logger, access_logger = _configure_logging()
    host = os.getenv('YEMAT_HOST', '0.0.0.0')
    port = int(os.getenv('YEMAT_PORT', '8080'))
    threads = int(os.getenv('YEMAT_THREADS', '12'))
    connection_limit = int(os.getenv('YEMAT_CONNECTION_LIMIT', '200'))
    channel_timeout = int(os.getenv('YEMAT_CHANNEL_TIMEOUT', '60'))

    logger.info('=' * 80)
    logger.info('Yemat waitress server start')
    logger.info('URL: http://%s:%s', host, port)
    logger.info('threads: %s', threads)
    logger.info('connection_limit: %s', connection_limit)
    logger.info('channel_timeout: %ss', channel_timeout)
    logger.info('=' * 80)
    access_logger.info('access log ready')

    serve(
        app,
        host=host,
        port=port,
        threads=threads,
        connection_limit=connection_limit,
        channel_timeout=channel_timeout,
    )


if __name__ == '__main__':
    main()
