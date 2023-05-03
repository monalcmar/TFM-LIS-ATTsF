# ========== Modulos Python =============
"""Librerias públiclas de python."""
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import text
import datetime
import re
# =========== Modulos propios ===========
from logger.logger import logger
from src.distribucion.etl import etl_distribucion
import dependencies as dp
from db.connection import engine, conn
from models.model import Personal
from models.model import Camion
from models.model import Wilaya
from models.model import Tipo_producto
from models.model import Tipo_vehiculo
from models.model import Distribucion
from utils.functions import columnas_texto
from utils.functions import busqueda_hoja


logger = logger()

logger.info('Inicio ETL Distribucion')

session = Session(engine)

# ===== Obtener maestros en dataframes ====

df_personal = pd.read_sql(session.query(Personal).statement, con=conn)
df_camion = pd.read_sql(session.query(Camion).statement, con=conn)
df_wilaya = pd.read_sql(session.query(Wilaya).statement, con=conn)
df_tipo_producto = pd.read_sql(session.query(Tipo_producto).statement, con=conn)
df_tipo_vehiculo = pd.read_sql(session.query(Tipo_vehiculo).statement, con=conn)
df_distribucion = pd.read_sql(session.query(Distribucion).statement, con=conn)

# ===== Obtener el último registro ¿en función de la fecha o del id_distribución? =====

ultimo_registro_distribucion = session.query(Distribucion).order_by(Distribucion.salida_fecha_hora.desc(),
                                                                    Distribucion.id_distribucion.desc()).first()
ultimo_id_distribucion = ultimo_registro_distribucion.id_distribucion
ultimo_salida_fecha_hora = ultimo_registro_distribucion.salida_fecha_hora
ultimo_no_serie = ultimo_registro_distribucion.no_serie

session.close()

# imprimir ultima fecha y último indice
print(ultimo_id_distribucion)
print(ultimo_salida_fecha_hora)
print(ultimo_no_serie)

# ===== Cargar nuevos datos =====

# crear path de origen de nuevos datos
path_input_nuevos = dp.rootFolder / 'data' / 'nuevos_datos'

# ===== Hacer merge de los nuevos datos con los maestros =====

# Leer datos nuevos TENIENDO EN CUENTA QUE ESTE SEA EL FORMATO

file_name = 'bbdd_distribucion.xlsx'

df_distribucion_nuevos = pd.read_excel(
    path_input_nuevos / file_name,
    sheet_name='base  datos 2024',  # optimizar búsqueda de hoja
    usecols='A, B, C, D, E, F, G, I, J, K, L, M, N, AC, AD',
    names=['no_serie', 'conductor', 'nombre_attsf', 'fecha_salida', 'hora_salida', 
           'fecha_llegada', 'hora_llegada', 'km_salida', 'km_llegada', 'km_totales',
           'tm', 'tipo_producto', 'wilaya', 'incidencias', 'observaciones'],
    header=3)
df_distribucion_nuevos = df_distribucion_nuevos.dropna(how='all')

# union de fechas y horas
df_distribucion_nuevos['salida_fecha_hora'] = pd.to_datetime(df_distribucion_nuevos['fecha_salida']).dt.date.astype(str) + ' ' + df_distribucion_nuevos['hora_salida'].astype(str)
df_distribucion_nuevos['llegada_fecha_hora'] = pd.to_datetime(df_distribucion_nuevos['fecha_llegada']).dt.date.astype(str) + ' ' + df_distribucion_nuevos['hora_llegada'].astype(str)

df_distribucion_nuevos['llegada_fecha_hora'] = pd.to_datetime(df_distribucion_nuevos['llegada_fecha_hora'],
                                                              format='%Y-%m-%d %H:%M:%S')
df_distribucion_nuevos['salida_fecha_hora'] = pd.to_datetime(df_distribucion_nuevos['salida_fecha_hora'],
                                                             format='%Y-%m-%d %H:%M:%S')

# gestion columnas de texto
df_distribucion_nuevos['conductor'] = columnas_texto(df_distribucion_nuevos['conductor'], 'title')
df_distribucion_nuevos['wilaya'] = columnas_texto(df_distribucion_nuevos['wilaya'], 'title')
df_distribucion_nuevos['nombre_attsf'] = columnas_texto(df_distribucion_nuevos['nombre_attsf'], 'upper')
df_distribucion_nuevos['tipo_producto'] = columnas_texto(df_distribucion_nuevos['tipo_producto'], 'upper')

# nos quedamos solo con los datos a partir de la última fecha y hora de registro
df_distribucion_nuevos = df_distribucion_nuevos[df_distribucion_nuevos['salida_fecha_hora'] >= ultimo_salida_fecha_hora]

# generación de merges de nuevos datos con los maestros
df_distribucion_nuevos = pd.merge(left=df_distribucion_nuevos, right=df_personal[['id_personal', 'nombre']],
                                  how='left', left_on='conductor', right_on='nombre')
df_distribucion_nuevos = pd.merge(left=df_distribucion_nuevos, right=df_camion[['id_camion', 'nombre_attsf']],
                                  how='left', on='nombre_attsf')
df_distribucion_nuevos = pd.merge(left=df_distribucion_nuevos, right=df_tipo_producto[['id_tipo_producto', 'tipo_producto']],
                                  how='left', on='tipo_producto')
df_distribucion_nuevos = pd.merge(left=df_distribucion_nuevos, right=df_wilaya[['id_wilaya', 'wilaya']], 
                                  how='left', on='wilaya')

# reconstrucción de dataframe de nuevos datos
d_distribucion_nuevos = {'id_conductor': df_distribucion_nuevos['id_conductor'],
                         'id_tipo_producto': df_distribucion_nuevos['id_tipo_producto'],
                         'id_camion': df_distribucion_nuevos['id_camion'],
                         'id_wilaya': df_distribucion_nuevos['id_wilaya'],
                         'no_serie': df_distribucion_nuevos['no_serie'],
                         'salida_fecha_hora': df_distribucion_nuevos['salida_fecha_hora'],
                         'llegada_fecha_hora': df_distribucion_nuevos['llegada_fecha_hora'],
                         'km_salida': df_distribucion_nuevos['km_salida'],
                         'km_llegada': df_distribucion_nuevos['km_llegada'],
                         'km_totales': df_distribucion_nuevos['km_totales'],
                         'tm': df_distribucion_nuevos['tm'],
                         'incidencias': df_distribucion_nuevos['incidencias'],
                         'observaciones': df_distribucion_nuevos['observaciones']}
df_distribucion_nuevos = pd.DataFrame(d_distribucion_nuevos)

# comprobar las fechas iguales a la última fecha
df_distribucion_fecha_igual = df_distribucion[(df_distribucion['salida_fecha_hora'] == ultimo_salida_fecha_hora)]
df_distribucion_fecha_igual = df_distribucion_fecha_igual.drop("id_distribucion", axis=1)

# Se comprubea si alguna de las filas del dataframe de nuevos datos esta dentro del ya existente y se descartan las que ya estaán incluidas
df_distribucion_nuevos = df_distribucion_nuevos[df_distribucion_nuevos.isin(df_distribucion_fecha_igual) == False].dropna(how='all')

# ===== modificar los indices =====

df_distribucion_nuevos.index += (ultimo_id_distribucion + 1)
df_distribucion_nuevos = df_distribucion_nuevos.reset_index(names='id_distribucion')
df_distribucion_nuevos

# ===== volcar nuevos datos en el servidor SQL =====

df_distribucion_nuevos.to_sql(name='tbl_distribucion', con=engine, schema='attsf', if_exists='append', index=False)

logger.info('Fin ETL Distribucion')