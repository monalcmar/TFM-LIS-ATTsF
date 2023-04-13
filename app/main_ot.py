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
tbl_averia = Base.classes.tbl_averia
tbl_frecuencia = Base.classes.tbl_frecuencia
tbl_taller = Base.classes.tbl_taller
tbl_ot = Base.classes.tbl_ot
tbl_camion = Base.classes.tbl_camion
tbl_tipo_vehiculo = Base.classes.tbl_tipo_vehiculo
tbl_wilaya = Base.classes.tbl_wilaya
tbl_repuesto = Base.classes.tbl_repuesto
tbl_mecanico = Base.classes.tbl_mecanico
tbl_disponibilidad = Base.classes.tbl_disponibilidad
tbl_distribucion = Base.classes.tbl_distribucion
tbl_tipo_producto = Base.classes.tbl_tipo_producto
tbl_conductor = Base.classes.tbl_conductor
tbl_tipo_taller = Base.classes.tbl_tipo_taller
tbl_tipo_ot = Base.classes.tbl_tipo_ot


# crear una instancia de la sesión ORM
session = Session(engine)

# crear DataFrames
df_tipo_ot = pd.read_sql(session.query(tbl_tipo_ot).statement, conn)

print(df_tipo_ot)


# # Convertir resultados ORM a DataFrame
# df = pd.DataFrame([vars(r) for r in resultado_orm])

logger.info('Fin ETL Distribucion')
