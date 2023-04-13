# ========== Modulos Python =============
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import text
# =========== Modulos propios ===========
from logger.logger import logger
from src.distribucion.etl import etl_distribucion
import dependencies as dp
from db.connection import engine, conn
from models.model import Conductor, Camion, Wilaya, Tipo_Producto, Tipo_Vehiculo


logger = logger()

logger.info('Inicio ETL Distribucion')

#df = pd.read_sql_query(text("SELECT * FROM attsf.tbl_wilaya"), con=conn)

## Crear la base de objetos y clases 

Base = automap_base()

# reflect the tables
Base.prepare(engine)

print(Base.classes.keys())


## se guardan los objetos en las respectiva variables 
Tbl_Distribucion = Base.classes.tbl_distribucion
Tbl_Conductor = Base.classes.attsf.tbl_conductor
Tbl_Camion = Base.classes.tbl_camion
Tbl_Wilaya = Base.classes.tbl_wilaya
Tbl_Tipo_Producto = Base.classes.tbl_tipo_producto
Tbl_Tipo_Vehiculo = Base.classes.tbl_tipo_vehiculo 



## ===== carga de tablas de maestros en dataframes =====
from sqlalchemy.orm import Session

# Crear un diccionario para almacenar los DataFrames
dataframes = {
    'df_conductor': None,
    'df_camion': None,
    'df_wilaya': None,
    'df_tipo_producto': None,
    'df_tipo_vehiculo': None,
}

# Crear una sesión
session = Session(engine)

# Iterar sobre las variables de las tablas
for nombre, tabla in zip(dataframes.keys(), [Conductor, Camion, Wilaya, Tipo_Producto, Tipo_Vehiculo]):
    # Hacer una consulta a la tabla
    result = session.query(tabla).all()

    # Crear una lista de diccionarios con los datos de la tabla
    data = []
    for row in result:
        data.append({c.name: getattr(row, c.name) for c in row.__table__.columns})

    # Crear un DataFrame de pandas a partir de la lista de diccionarios
    df = pd.DataFrame(data)

    # Asignar el DataFrame al diccionario
    dataframes[nombre] = df

# Cerrar la sesión
session.close()

## ===== carga de tabla existente de hechos =====
from sqlalchemy.orm import Session

# Crear una sesión
session = Session(engine)

# Hacer una consulta a la tabla tbl_distribucion
result = session.query(Distribucion).all()

data = []
for row in result:
    data.append({c.name: getattr(row, c.name) for c in row.__table__.columns})

# Crear un DataFrame de pandas a partir de la lista de diccionarios
df_distribucion = pd.DataFrame(data)

# Imprimir el DataFrame
#print(df)

# Cerrar la sesión
session.close()

## ===== obtener el último registro ¿en función de la fecha o del id_distribución? =====

## ===== Cargar nuevos datos =====

## ===== Hacer merge de los nuevos datos con los maestros =====


## ===== modificar los indices =====


## ===== volcar nuevos datos en el servidor SQL =====

logger.info('Fin ETL Distribucion')
