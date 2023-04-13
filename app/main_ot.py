import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
# =========== Modulos propios ===========
from logger.logger import logger
#from src.ot.etl import etl.ot
import dependencies as dp
from db.connection import engine, conn


logger = logger()

logger.info('Inicio ETL Distribucion')

#etl_ot()

# Crear la clase base ORM
Base = automap_base()
# Reflejar las tablas de la base de datos en la clase base ORM
Base.prepare(engine, reflect=True)

# tablas = Base.classes.keys()
# print(tablas)

# Acceder a las tablas de la base de datos a través de su clase ORM
averia = Base.classes.tbl_averia
frecuencia = Base.classes.tbl_frecuencia
taller = Base.classes.tbl_taller
ot = Base.classes.tbl_ot
camion = Base.classes.tbl_camion
tipo_vehiculo = Base.classes.tbl_tipo_vehiculo
wilaya = Base.classes.tbl_wilaya
repuesto = Base.classes.tbl_repuesto
mecanico = Base.classes.tbl_mecanico
disponibilidad = Base.classes.tbl_disponibilidad
distribucion = Base.classes.tbl_distribucion
tipo_producto = Base.classes.tbl_tipo_producto
conductor = Base.classes.tbl_conductor
tipo_taller = Base.classes.tbl_tipo_taller
tipo_ot = Base.classes.tbl_tipo_ot


# crear una instancia de la sesión ORM
session = Session(engine)

# crear DataFrames
df_tipo_ot = pd.read_sql(session.query(tipo_ot).statement, conn)

print(df_tipo_ot)


# # Convertir resultados ORM a DataFrame
# df = pd.DataFrame([vars(r) for r in resultado_orm])

logger.info('Fin ETL Distribucion')
