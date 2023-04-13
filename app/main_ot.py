import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
# =========== Modulos propios ===========
from logger.logger import logger
# from src.ot.etl import etl.ot
import dependencies as dp
from db.connection import engine, conn
from models.model import Tipo_ot


logger = logger()

logger.info('Inicio ETL Distribucion')

# etl_ot()

# # Crear la clase base ORM
# Base = automap_base()
# # Reflejar las tablas de la base de datos en la clase base ORM
# Base.prepare(engine, reflect=True)

# # tablas = Base.classes.keys()
# # print(tablas)

# # Acceder a las tablas de la base de datos a través de su clase ORM
# Averia = Base.classes.tbl_averia
# Frecuencia = Base.classes.tbl_frecuencia
# Taller = Base.classes.tbl_taller
# Ot = Base.classes.tbl_ot
# Camion = Base.classes.tbl_camion
# Tipo_vehiculo = Base.classes.tbl_tipo_vehiculo
# Wilaya = Base.classes.tbl_wilaya
# Repuesto = Base.classes.tbl_repuesto
# Mecanico = Base.classes.tbl_mecanico
# Disponibilidad = Base.classes.tbl_disponibilidad
# Distribucion = Base.classes.tbl_distribucion
# Tipo_producto = Base.classes.tbl_tipo_producto
# Conductor = Base.classes.tbl_conductor
# Tipo_taller = Base.classes.tbl_tipo_taller
# Tipo_ot = Base.classes.tbl_tipo_ot


# crear una instancia de la sesión ORM
session = Session(engine)

# crear DataFrames
df_tipo_ot = pd.read_sql(session.query(Tipo_ot).statement, conn)

print(df_tipo_ot)


# # Convertir resultados ORM a DataFrame
# df = pd.DataFrame([vars(r) for r in resultado_orm])

logger.info('Fin ETL Distribucion')
