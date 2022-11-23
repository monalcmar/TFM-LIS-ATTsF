from fastapi import FastAPI

# =========== Modulos propios ===========
from logger.logger import logger
from routers import api
import dependencies


logger = logger()

app = FastAPI()  # EN PRODUCCIÓN HABRÍA QUE QUITAR LO DEL SWAGGER
app.include_router(api.router)

logger.info('API Iniciada')
