# =========== Modulos propios ===========
from logger.logger import logger
from src.distribucion.etl import etl_distribucion
import dependencies as dp



logger = logger()

logger.info('Inicio ETL Distribucion')

etl_distribucion()

logger.info('Fin ETL Distribucion')
