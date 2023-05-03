import pandas as pd
import numpy as np
from sqlalchemy.orm import Session

# =========== Modulos propios ===========
import dependencies as dp
from db.connection import engine
from utils.functions import check_relations, update_db_table, capitalize_df
from models.model import Tipo_ot
from models.model import Frecuencia
from models.model import Tipo_taller
from models.model import Averia
from models.model import Taller
from models.model import Repuesto
from models.model import Personal
from models.model import Camion
from models.model import Tipo_producto
from models.model import Wilaya
from models.model import Tipo_vehiculo


path_input = dp.rootFolder / 'data' / 'maestros'

dp.logger.info('Inicio ETL Maestros')
# ==============================================================================
# Lectura DF
# ==============================================================================
df_camion = pd.read_excel(
    path_input / 'maestros.xlsx',
    sheet_name='camion',
    usecols='A:Q',
    parse_dates=['fecha_alta', 'fecha_baja']
)
df_wilaya = pd.read_excel(
    path_input / 'maestros.xlsx',
    sheet_name='wilaya',
    usecols='A:E'
)
df_taller = pd.read_excel(
    path_input / 'maestros.xlsx',
    sheet_name='taller',
    usecols='A:G'
)
df_personal = pd.read_excel(
    path_input / 'maestros.xlsx',
    sheet_name='personal',
    usecols='A:D'
)
df_averia = pd.read_excel(
    path_input / 'maestros.xlsx',
    sheet_name='averia',
    usecols='A:B'
)
df_repuesto = pd.read_excel(
    path_input / 'maestros.xlsx',
    sheet_name='repuesto',
    usecols='A:C'
)
df_frecuencia = pd.read_excel(
    path_input / 'maestros.xlsx',
    sheet_name='frecuencia',
    usecols='A:B'
)
df_tipo_ot = pd.read_excel(
    path_input / 'maestros.xlsx',
    sheet_name='tipo_ot',
    usecols='A:B'
)
df_tipo_producto = pd.read_excel(
    path_input / 'maestros.xlsx',
    sheet_name='tipo_producto',
    usecols='A:C'
)
df_tipo_taller = pd.read_excel(
    path_input / 'maestros.xlsx',
    sheet_name='tipo_taller',
    usecols='A:C'
)
df_tipo_vehiculo = pd.read_excel(
    path_input / 'maestros.xlsx',
    sheet_name='tipo_vehiculo',
    usecols='A:C'
)

# ==============================================================================
# camion
# ==============================================================================
df_camion = check_relations('camion', df_camion, df_wilaya, 'id_wilaya')
df_camion = check_relations('camion', df_camion, df_tipo_vehiculo, 'id_tipo_vehiculo')

# ==============================================================================
# taller
# ==============================================================================
df_taller = check_relations('taller', df_taller, df_wilaya, 'id_wilaya')
df_taller = check_relations('taller', df_taller, df_tipo_taller, 'id_tipo_taller')

# ==============================================================================
# capitalize_df()
# ==============================================================================
df_averia = capitalize_df(df_averia)
df_repuesto = capitalize_df(df_repuesto)

# ==============================================================================
# Cambiar NaN por None
# ==============================================================================
df_wilaya = df_wilaya.replace({np.NaN: None})
df_taller = df_taller.replace({np.NaN: None})
df_personal = df_personal.replace({np.NaN: None})
df_averia = df_averia.replace({np.NaN: None})
df_repuesto = df_repuesto.replace({np.NaN: None})
df_frecuencia = df_frecuencia.replace({np.NaN: None})
df_tipo_ot = df_tipo_ot.replace({np.NaN: None})
df_tipo_producto = df_tipo_producto.replace({np.NaN: None})
df_tipo_taller = df_tipo_taller.replace({np.NaN: None})
df_tipo_vehiculo = df_tipo_vehiculo.replace({np.NaN: None})
df_camion = df_camion.replace(
    {
        np.NaN: None,
        pd.NaT: None
    }
)

# ==============================================================================
# df.to_dict
# ==============================================================================
dict_camion = df_camion.to_dict(orient="records")
dict_wilaya = df_wilaya.to_dict(orient="records")
dict_taller = df_taller.to_dict(orient="records")
dict_personal = df_personal.to_dict(orient="records")
dict_averia = df_averia.to_dict(orient="records")
dict_repuesto = df_repuesto.to_dict(orient="records")
dict_frecuencia = df_frecuencia.to_dict(orient="records")
dict_tipo_ot = df_tipo_ot.to_dict(orient="records")
dict_tipo_producto = df_tipo_producto.to_dict(orient="records")
dict_tipo_taller = df_tipo_taller.to_dict(orient="records")
dict_tipo_vehiculo = df_tipo_vehiculo.to_dict(orient="records")

# ==============================================================================
# Carga de los datos
# ==============================================================================
try:
    session = Session(engine)
except Exception as e:
    dp.logger.error("Ocurrió un error:", e)

update_db_table(dict_wilaya, Wilaya, 'Wilaya', session)
update_db_table(dict_personal, Personal, 'Personal', session)
update_db_table(dict_averia, Averia, 'Averia', session)
update_db_table(dict_repuesto, Repuesto, 'Repuesto', session)
update_db_table(dict_frecuencia, Frecuencia, 'Frecuencia', session)
update_db_table(dict_tipo_ot, Tipo_ot, 'Tipo OT', session)
update_db_table(dict_tipo_producto, Tipo_producto, 'Tipo Producto', session)
update_db_table(dict_tipo_taller, Tipo_taller, 'Tipo Taller', session)
update_db_table(dict_tipo_vehiculo, Tipo_vehiculo, 'Tipo Vehiculo', session)
update_db_table(dict_camion, Camion, 'Camion', session)
update_db_table(dict_taller, Taller, 'Taller', session)

session.close()
dp.logger.info('Fin ETL Maestros')
