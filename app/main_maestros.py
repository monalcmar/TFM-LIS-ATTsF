import pandas as pd
import numpy as np
from sqlalchemy.orm import Session

# =========== Modulos propios ===========
import dependencies as dp
from db.connection import engine
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


def elements_not_in_list(list1, list2):
    """
    Comprobamos que los elementos de la lista 2 están en la lista 1. Si hay
    alguno que no está, se hace retornan.
    """
    not_in_list1 = [elem for elem in list2 if elem not in list1]

    if len(not_in_list1) != 0:
        return not_in_list1
    

def check_relations(db_table, df, df_master, column):
    """
    Se hace un LOG de los elementos que tiene algun id de otra tabla mal referenciado
    """
    out_elemnts = elements_not_in_list(df_master.iloc[:, 0].tolist(), df[column].tolist())
    if out_elemnts is not None:
        df_filtered = df[df[column].isin(out_elemnts)]
        dp.logger.warning(f'En la tabla {db_table}, {column} mal referenciado para los siguientes elementos: ' + ', '.join(str(id) for id in df_filtered.iloc[:, 1]))
        return df[~df[column].isin(out_elemnts)]
    return df


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
df_camion = df_camion.replace(
    {
        np.NaN: None,
        pd.NaT: None
    }
)

# ==============================================================================
# camion
# ==============================================================================
df_taller = check_relations('taller', df_taller, df_wilaya, 'id_wilaya')
df_taller = check_relations('taller', df_taller, df_tipo_taller, 'id_tipo_taller')

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

    for row in dict_wilaya:
        rec = Wilaya(**row)
        session.merge(rec)
    dp.logger.info('Actualización tabla Wilaya')

    for row in dict_personal:
        rec = Personal(**row)
        session.merge(rec)
    dp.logger.info('Actualización tabla Personal')

    for row in dict_averia:
        rec = Averia(**row)
        session.merge(rec)
    dp.logger.info('Actualización tabla Averia')

    for row in dict_repuesto:
        rec = Repuesto(**row)
        session.merge(rec)
    dp.logger.info('Actualización tabla Repuesto')

    for row in dict_frecuencia:
        rec = Frecuencia(**row)
        session.merge(rec)
    dp.logger.info('Actualización tabla Frecuencia')

    for row in dict_tipo_ot:
        rec = Tipo_ot(**row)
        session.merge(rec)
    dp.logger.info('Actualización tabla Tipo OT')

    for row in dict_tipo_producto:
        rec = Tipo_producto(**row)
        session.merge(rec)
    dp.logger.info('Actualización tabla Tipo Producto')

    for row in dict_tipo_taller:
        rec = Tipo_taller(**row)
        session.merge(rec)
    dp.logger.info('Actualización tabla Tipo Taller')

    for row in dict_tipo_vehiculo:
        rec = Tipo_vehiculo(**row)
        session.merge(rec)
    dp.logger.info('Actualización tabla Tipo Vehiculo')

    for row in dict_camion:
        rec = Camion(**row)
        session.merge(rec)
    dp.logger.info('Actualización tabla Camion')

    for row in dict_taller:
        rec = Taller(**row)
        session.merge(rec)
    dp.logger.info('Actualización tabla Taller')
   

except Exception as e:
    dp.logger.error("Ocurrió un error:", e)
    session.rollback()

finally:
    # Cerrar la sesión
    session.close()
    dp.logger.info('Fin ETL Maestros')

