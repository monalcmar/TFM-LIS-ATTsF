import pandas as pd
import numpy as np
import re
import datetime
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
# =========== Modulos propios ===========
from logger.logger import logger
# from src.ot.etl import etl.ot
import dependencies as dp
from db.connection import engine, conn
from models.model import Tipo_ot
from models.model import Frecuencia
from models.model import Tipo_taller
from models.model import Averia
from models.model import Taller
from models.model import Repuesto
from models.model import Personal
from models.model import Camion
from models.model import Ot
from models.model import Wilaya
from utils.functions import columnas_texto
from utils.functions import asociar_wilaya_taller
from utils.functions import pasar_a_int
from utils.functions import replace_camion
from utils.functions import busqueda_hoja
from utils.functions import find_file

path_input = dp.rootFolder / 'data'

dp.logger.info('Inicio ETL Ot')

# Crear una instancia de la sesión ORM
session = Session(engine)

# Crear DataFrames
df_tipo_ot = pd.read_sql(session.query(Tipo_ot).statement, conn)
df_frecuencia = pd.read_sql(session.query(Frecuencia).statement, conn)
df_tipo_taller = pd.read_sql(session.query(Tipo_taller).statement, conn)
df_averia = pd.read_sql(session.query(Averia).statement, conn)
df_taller = pd.read_sql(session.query(Taller).statement, conn)
df_repuesto = pd.read_sql(session.query(Repuesto).statement, conn)
df_personal = pd.read_sql(session.query(Personal).statement, conn)
df_camion = pd.read_sql(session.query(Camion).statement, conn)
df_wilaya = pd.read_sql(session.query(Wilaya).statement, conn)
df_ot = pd.read_sql(session.query(Ot).statement, conn)

# # Obtener la ultima fecha, el ultimo id_ot y los ots que tienen la ultima fecha
# ultima_fecha = session.query(Ot.fecha_inicio).filter(Ot.fecha_inicio != None).order_by(Ot.fecha_inicio.desc()).first()[0]
# ultima_id_ot = session.query(Ot.id_ot).order_by(Ot.id_ot.desc()).first()[0]

# # Se toman las ots que tienen la ultima fecha
# ot_ultima_fecha = [ot[0] for ot in session.query(Ot.ot).filter(Ot.fecha_inicio == ultima_fecha).all()]

session.close()

df_ot['ot'] = df_ot['ot'].astype(float).astype(int).astype(str)

name_agua = 'UNHCR'
file_agua = find_file(path_input, name_agua)

# ---------------------------------------- INFORME ----------------------------------------
hoja_agua_informe = 'Base de datos Correctiva INFORM'
sheet_agua_informe = busqueda_hoja(file_agua, hoja_agua_informe)

# Se define el dataframe de ot averia (se usara mas adelante)
df_otc_averia_agua = pd.read_excel(
    file_agua,
    sheet_name=sheet_agua_informe,
    usecols='A, K:BT',
    header=0
)

df_otc_averia_agua = df_otc_averia_agua.rename(columns={'NºOTC': 'ot'})
df_otc_averia_agua.columns = df_otc_averia_agua.columns.str.lower()

# Se eliminan las filas con valores nan en todas las columnas
df_otc_averia_agua.dropna(how='all', inplace=True)
# No tienen numero de ot
df_otc_averia_agua.dropna(subset=['ot'], inplace=True)

# Se crea la lista de averias
df_tipo_averia_agua = df_otc_averia_agua.drop('ot', axis=1)
averias_agua = df_tipo_averia_agua.columns.tolist()

# Se juntan e invierten columnas
df_ot_agua_averia = pd.melt(
    df_otc_averia_agua,
    id_vars='ot',
    value_vars=averias_agua
).loc[:, ['ot', 'variable']].rename(columns={'variable': 'averia'})

# Se limpian los datos
df_ot_agua_averia['ot'] = df_ot_agua_averia['ot'].astype(int).astype(str)
df_ot_agua_averia = df_ot_agua_averia.replace({'averia': {'electrica': 'elec, vehiculos'}})
df_ot_agua_averia['averia'] = columnas_texto(df_ot_agua_averia['averia'], 'capitalize')

# Camiones de NO alimentos
camiones = pd.merge(df_camion.loc[:, ['id_camion', 'id_tipo_vehiculo']], df_ot.loc[:, ['id_camion']], how='left', on='id_camion')

camiones_noalimentos = camiones.loc[camiones['id_tipo_vehiculo'] != 3, 'id_camion'].unique()
# Dataframe de df_ot con camiones que NO son de alimentos
df_ot_noalimento = df_ot.loc[df_ot['id_camion'].isin(camiones_noalimentos)]

# Merge ot
df_ot_agua_averia = pd.merge(df_ot_agua_averia, df_ot_noalimento.loc[:, ['id_ot', 'ot']], how='left', on='ot')
# Se elimina la columna ot
#df_ot_agua_averia = df_ot_agua_averia.drop('ot', axis=1)

# Merge averia
df_ot_agua_averia = pd.merge(df_ot_agua_averia, df_averia.loc[:, ['id_averia', 'averia']], how='left', on='averia')
# Se elimina la columna averia
df_ot_agua_averia = df_ot_agua_averia.drop('averia', axis=1)

# Obtenemos las filas con valores NaN en 'id_ot'
filas_nulas = df_ot_agua_averia[df_ot_agua_averia['id_ot'].isna()] #5518 ots que no coinciden!!

# Se eliminan los id_ot que no tienen ot asociada
#df_ot_agua_averia.dropna(subset=['id_ot'], inplace=True)
#df_ot_agua_averia['id_ot'] = df_ot_agua_averia['id_ot'].astype(int)

# Mostramos el resultado
print(filas_nulas['ot'].unique())
