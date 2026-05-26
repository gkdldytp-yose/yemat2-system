import logging
import os
import re
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import sys

from waitress import serve

from app import app


ANSI_RESET = '\033[0m'
ANSI_DIM = '\033[2m'
ANSI_BOLD = '\033[1m'
ANSI_BLUE = '\033[34m'
ANSI_CYAN = '\033[36m'
ANSI_GREEN = '\033[32m'
ANSI_MAGENTA = '\033[35m'
ANSI_RED = '\033[31m'
ANSI_YELLOW = '\033[33m'
ANSI_GRAY = '\033[90m'


def _color(text, color):
    return f'{color}{text}{ANSI_RESET}'


def _status_color(status_code):
    try:
        code = int(status_code)
    except (TypeError, ValueError):
        return ANSI_GRAY
    if code >= 500:
        return ANSI_RED
    if code >= 400:
        return ANSI_YELLOW
    if code >= 300:
        return ANSI_CYAN
    if code >= 200:
        return ANSI_GREEN
    return ANSI_GRAY


class AccessConsoleFormatter(logging.Formatter):
    _line_re = re.compile(
        r'^(?P<ip>.*?) \| (?P<user>.*?) \| (?P<workplace>.*?) \| '
        r'(?P<method>.*?) \| (?P<status>.*?) \| (?P<endpoint>.*?) \| '
        r'(?P<path>.*?) \| (?P<elapsed>.*?) \| (?P<referer>.*)$'
    )

    def format(self, record):
        message = record.getMessage()
        parsed = self._line_re.match(message)
        timestamp = self.formatTime(record, self.datefmt)
        if not parsed:
            return f'{_color(timestamp, ANSI_GRAY)} {_color("[ACCESS]", ANSI_MAGENTA)} {message}'

        data = {key: (value.strip() or '-') for key, value in parsed.groupdict().items()}
        status = data['status']
        status_color = _status_color(status)
        method_color = {
            'GET': ANSI_BLUE,
            'POST': ANSI_CYAN,
            'PUT': ANSI_YELLOW,
            'PATCH': ANSI_YELLOW,
            'DELETE': ANSI_RED,
        }.get(data['method'].upper(), ANSI_GRAY)

        left = (
            f'{_color(timestamp, ANSI_GRAY)} '
            f'{_color("[ACCESS]", ANSI_MAGENTA)} '
            f'{_color(data["method"].upper(), method_color):<16} '
            f'{_color(status, status_color):<14} '
            f'{_color(data["elapsed"], ANSI_DIM):>12}'
        )
        middle = (
            f' {_color(data["path"], ANSI_BOLD)} '
            f'{_color("->", ANSI_GRAY)} '
            f'{_color(data["endpoint"], ANSI_CYAN)}'
        )
        right = (
            f' {_color("|", ANSI_GRAY)} user={_color(data["user"], ANSI_GREEN)}'
            f' workplace={_color(data["workplace"], ANSI_BLUE)}'
            f' ip={_color(data["ip"], ANSI_GRAY)}'
        )
        if data['referer'] != '-':
            right += f' {_color("|", ANSI_GRAY)} ref={_color(data["referer"], ANSI_DIM)}'
        return left + middle + right


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
        access_console_handler.setFormatter(AccessConsoleFormatter(datefmt='%Y-%m-%d %H:%M:%S'))
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
