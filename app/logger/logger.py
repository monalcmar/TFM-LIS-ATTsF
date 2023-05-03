import logging
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler


def logger():
    path = (Path(__file__).parent / 'log')

    if not path.exists():
        path.mkdir(parents=True, exist_ok=False)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    logHandler = TimedRotatingFileHandler(
        path / 'log_app.log',
        when="midnight",
        interval=1,
        backupCount=90,
        encoding='utf8')
    logHandler.suffix = "%Y-%m-%d"
    logHandler.setFormatter(formatter)

    streamLogger = logging.StreamHandler()
    streamLogger.setLevel(logging.INFO)
    streamLogger.setFormatter(formatter)

    logger.addHandler(logHandler)
    logger.addHandler(streamLogger)

    return logger

# Meter como parametros logger y nivel de log
# Meter el tema de loggear los print