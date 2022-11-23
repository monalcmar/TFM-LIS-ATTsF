import os
import yaml
from pathlib import Path
from dotenv import load_dotenv

from logger.logger import logger


rootFolder = Path(__file__).parent

load_dotenv(rootFolder / '.env')

enviroment = 'LOCAL'

with open(Path(__file__).parent / 'config' / 'config.yml') as config_file:
    config = yaml.load(config_file, Loader=yaml.FullLoader)

logger = logger()
