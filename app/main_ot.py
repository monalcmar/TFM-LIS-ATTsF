import pandas as pd
import numpy as np
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

path_input = dp.rootFolder / 'data' / 'raw'
path_output = dp.rootFolder / 'data' / 'processed'


logger = logger()

logger.info('Inicio ETL Ot')

# etl_ot()

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
# Obtener la ultima fecha y el ultimo id_ot
ultima_fecha = session.query(Ot.fecha_inicio).filter(Ot.fecha_inicio != None).order_by(Ot.fecha_inicio.desc()).first()[0]
ultima_id_ot = session.query(Ot.id_ot).order_by(Ot.id_ot.desc()).first()[0]
print(df_personal)

# Funciones
def limpiar_capitalize(dataframe, columna):
    """
    Limpia el texto.

    Quita los espacios de adelante y atras y lo pasa
    a capitalize.
    """
    cadena_capitalize = dataframe[columna].str.strip().str.capitalize()
    return cadena_capitalize


def limpiar_upper(dataframe, columna):
    """
    Limpia el texto.

    Quita los espacios de adelante y atras y lo pasa
    a upper.
    """
    cadena_upper = dataframe[columna].str.strip().str.upper()
    return cadena_upper


def limpiar_lower(dataframe, columna):
    """
    Limpia el texto.

    Quita los espacios de adelante y atras y lo pasa
    a lower.
    """
    cadena_lower = dataframe[columna].str.strip().str.lower()
    return cadena_lower


# ################ PREVENTIVO ################
# NO HAY COLUMNA DE MECANICOS
# SE PONE EL COSTE? OJO CON LAS FECHAS!

# Se extrae la información del excel de ot preventivo
df_prev = pd.read_excel(
    path_input / 'BASE DE DATOS ---------',
    sheet_name='Base de datos Preventiva',
    usecols='A, B, G, H, I:J, L:Z, AA',
    skiprows=5,
    parse_dates=['fecha_inicio', 'fecha_fin'],
    names=['ot', 'camion', 'taller', 'frecuencia', 'fecha_inicio', 'fecha_fin',
            'aceite motor', 'anticongelante', 'liquido de embrague',
            'liquido direccion', 'liquido de freno', 'agua destilada',
            'aceite caja cambios', 'acido baterias', 'grasa', 'fitro de aceite',
            'filtro de aire', 'filtro de gasoil', 'filtro hidraulico',
            'filtro separador de gasoil', 'pre-filtro de gasoil',
            'coste mantenimiento']
)

# Seleccionar las filas que tienen fecha posterior a la fecha de referencia
df_ot_prev = df_prev.loc[df_prev['fecha_inicio'] >= ultima_fecha]  # OJOOOO, si hay dos fechas iguales o asi

# Se eliminan las filas con valores nan en todas las columnas
df_ot_prev.dropna(how='all', inplace=True)
# si no tiene numero de ot tampoco tiene informacion
df_ot_prev.dropna(subset=['ot'], inplace=True)
# camiones que son vacios y su informacion tambien
df_ot_prev.dropna(subset=['camion'], inplace=True)

# Se cambian los nan por un tipo de frecuencia 'No frecuencia'
df_ot_prev['frecuencia'] = df_ot_prev['frecuencia'].fillna('No frecuencia')


# Se limpian los datos
df_ot_prev['ot'] = limpiar_upper(df_ot_prev, 'ot')

df_ot_prev['camion'] = limpiar_upper(df_ot_prev, 'camion')

df_ot_prev['taller'] = limpiar_upper(df_ot_prev, 'taller')

# df_ot_prev['mecanico'] = limpiar_capitalize(df_ot_prev, 'mecanico')

df_ot_prev['frecuencia'] = limpiar_capitalize(df_ot_prev, 'frecuencia')

# df_ot_prev['coste'] = df_ot_prev['coste'].astype(float)

columnas_litros = ['aceite', 'anticongelante', 'embrague', 'h direccion', 'a/destil',
                   'a. caja', 'baterias']
df_ot_prev[columnas_litros] = df_ot_prev[columnas_litros].astype(int)

columnas_kilos = ['grasa']
df_ot_prev[columnas_kilos] = df_ot_prev[columnas_kilos].astype(int)

columnas_unidades = ['ruedas', 'lamparas', 'd/tacgfo', 's. lava', 'j. carroceria', 'calcho']
df_ot_prev[columnas_unidades] = df_ot_prev[columnas_unidades].astype(int)

# Se quitan los duplicatos OJO
# df_ot_prev = df_ot_prev.drop_duplicates()

# Se reeplazan los valores de taller
df_ot_prev = df_ot_prev.replace({'taller': {'ATC RABUNI ': 'CLM', 'TR AAIUN': 'Aaiun',
                                 'TR AUSERD': 'Auserd', 'TR BOUJDUR': 'Bojador',
                                 'TR DAJLA': 'Dajla', 'TR SMARA': 'Smara',
                                 'TR TRANSPORT': 'BDT'}})

# Se resetea el index por los valores eliminados
df_ot_prev.reset_index(inplace=True, drop=True)

# Crear una columna de tipo_ot = 'Preventivo'
df_ot_prev['tipo_ot'] = 'Preventivo'

# Crear una columna de descripcion_trabajo_solicitado	
df_ot_prev['descripcion_trabajo_solicitado'] = ''

# Crear una columna de descripcion_trabajo_realizado	
df_ot_prev['descripcion_trabajo_realizado'] = ''

# Crear una columna de observacion
df_ot_prev['observacion'] = ''

# Se define el dataframe de ot repuesto (se usara mas adelante) ORDENAR Y CAMBIAR NOMBRES
df_ot_repuesto = df_ot_prev[['aceite motor', 'anticongelante', 'liquido de embrague',
                             'liquido direccion', 'liquido de freno', 'agua destilada',
                             'aceite caja cambios', 'acido baterias', 'grasa', 'fitro de aceite',
                             'filtro de aire', 'filtro de gasoil', 'filtro hidraulico',
                             'filtro separador de gasoil', 'pre-filtro de gasoil']]

# Se toman las columnas deseadas
df_ot_prev = df_ot_prev[['ot', 'camion', 'tipo_ot', 'frecuencia', 'taller',
                         'mecanico', 'fecha_inicio', 'fecha_fin',
                         'descripcion_trabajo_solicitado',
                         'descripcion_trabajo_realizado' 'observacion']]


# ################ CORRECTIVO ################
# PROBLEMA CON LOS MECANICOS, VA A VER SOLO ESAS AVERIAS?

# Se extrae la información del excel de ot correctivo
df_corr = pd.read_excel(
    path_input / 'BASE DE DATOS -------',
    sheet_name='Base de datos Correctiva',
    usecols='A, B, C, E, I:J, L:P, Q, R, S',
    skiprows=4,
    parse_dates=['fecha_inicio', 'fecha_fin'],
    names=['camion', 'taller', 'ot', 'mecanico', 'fecha_inicio', 'fecha_fin', 'chasis',
            'carroceria', 'ruedas', 'mecanica', 'elec, vehiculos',
            'descripcion_trabajo_solicitado', 'descripcion_trabajo_realizado', 'observacion']
)

# Seleccionar las filas que tienen fecha igual o posterior a la fecha de referencia
df_ot_corr = df_corr.loc[df_corr['fecha_inicio'] >= ultima_fecha]  # OJOOOO, si hay dos fechas iguales o asi

# Se eliminan las filas con valores nan en todas las columnas
df_ot_corr.dropna(how='all', inplace=True)
# si no tiene numero de ot tampoco tiene informacion
df_ot_corr.dropna(subset=['ot'], inplace=True)
# camiones que son vacios y su informacion tambien
df_ot_corr.dropna(subset=['camion'], inplace=True)

# Se limpian los datos
df_ot_corr['ot'] = limpiar_upper(df_ot_corr, 'ot')

df_ot_corr['camion'] = limpiar_upper(df_ot_corr, 'camion')

df_ot_corr['mecanico'] = limpiar_capitalize(df_ot_corr, 'mecanico')

df_ot_corr['descripcion_trabajo_solicitado'] = limpiar_capitalize(df_ot_corr, 'descripcion_trabajo_solicitado')

df_ot_corr['descripcion_trabajo_realizado'] = limpiar_capitalize(df_ot_corr, 'descripcion_trabajo_realizado')

df_ot_corr['observacion'] = limpiar_capitalize(df_ot_corr, 'observacion')

columnas_repuestos = ['chasis', 'carroceria', 'ruedas', 'mecanica', 'elec, vehiculos']
df_ot_corr[columnas_repuestos] = df_ot_corr[columnas_repuestos].astype(int)

# Eliminar los duplicados OJO
# df_ot_corr = df_ot_corr.drop_duplicates()

# Se reeplazan los valores de taller
df_ot_corr = df_ot_corr.replace({'taller': {'ATC RABUNI': 'CLM', 'TR AAIUN': 'Aaiun',
                                 'TR AUSERD': 'Auserd', 'TR BOUJDUR': 'Bojador',
                                 'TR DAJLA': 'Dajla', 'TR SMARA': 'Smara',
                                 'TR TRANSPORT': 'BDT'}})

# Se resetea el index por los valores eliminados
df_ot_corr.reset_index(inplace=True, drop=True)

# Crear una columna de frecuencia = 'No frecuencia'
df_ot_corr['frecuencia'] = 'No frecuencia'

# Crear una columna de tipo_ot = 'Correctivo'
df_ot_corr['tipo_ot'] = 'Correctivo'

# Crear una columna de taller
df_ot_corr['taller'] = ''

# REVISAR LOS MECANICOS CAMBIADOS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Se reemplazan los nombres de los mecanicos
df_ot_corr = df_ot_corr.replace({'mecanico': {'Saleh': 'Saleh Mohamed Salma',
                                 'Mohamed lamin': 'Mohamed Lamin Hussein',
                                 'Bal-lal hammu': 'Bel-lal Hammu', 
                                 'Ahmed salek andal': 'Ahmed Salec',
                                 'Salek ali': 'Salec Ali',
                                 'Ali': 'Ali Eluali',
                                 'Mohamed abdalahi': 'Mohamed-Abdalahe Mohamed-Bachir',
                                 'Laulad': 'Laulad Mohamed-Salem',
                                 'Sidi alal': 'Sidi alal Brahim',
                                 'Jatri mohamed': 'Jatri  Mohamed',
                                 'Todos': '---------',
                                 'Mec transporte ': '-------',
                                 'Conductor': '------',
                                 'Lamni': '-------',
                                 'Mohamed': '------',
                                 'Moh lamin': 'Mohamed Lamin Mustafa',
                                 'Ali salem': 'Alisalem Mohamed-Salem'}})

# Se define el dataframe de ot averia (se usara mas adelante)
df_ot_averia = df_ot_corr[['ot', 'chasis', 'carroceria', 'ruedas',
                           'mecanica', 'elec, vehiculos']]

# Se toman las columnas deseadas
df_ot_corr = df_ot_corr[['ot', 'camion', 'tipo_ot', 'frecuencia', 'taller',
                         'mecanico', 'fecha_inicio', 'fecha_fin',
                         'descripcion_trabajo_solicitado', 
                         'descripcion_trabajo_realizado', 'observacion']]


# ################ UNION ################

# Concat de ambos dataframes
df_ot = pd.concat([df_ot_corr, df_ot_prev], axis=0)

# Se toman las columnas deseadas (FALTA LA COLUMNA TALLER QUE AUN NO SE TIENE INFORMACION!!!!!!: entre camion y tipo_ot)
df_ot = df_ot[['ot', 'camion', 'tipo_ot', 'frecuencia', 'taller', 'mecanico',
               'fecha_inicio', 'fecha_fin', 'descripcion_trabajo_solicitado', 
               'descripcion_trabajo_realizado', 'observacion']]

# Se quitan los indices anteriores ya que se repiten cada anho
df_ot.reset_index(inplace=True, drop=True)

# Ordenar por fecha
df_ot = df_ot.sort_values(by='fecha_inicio')

# Numero de filas del dataframe
num_filas = df_ot.shape[0]

# Generar una Serie desde el numero ultima_id_ot
num_ot = pd.Series(range(ultima_id_ot, num_filas + ultima_id_ot))
df_ot['id_ot'] = num_ot


# ################ MERGES ################

# Merge de ot e id_camion
df_ot = pd.merge(df_ot, df_camion.loc[:, ['id_camion', 'camion']], how='left', on='camion')
# Eliminar columna camion
df_ot = df_ot.drop('camion', axis=1)

# Merge de ot e id_taller
df_ot = pd.merge(df_ot, df_taller.loc[:, ['id_taller', 'taller']], how='left', on='taller')
# Eliminar columna taller
df_ot = df_ot.drop('taller', axis=1)
# Como salen Nan hay que pasarlo a int
df_ot['taller'] = df_ot['taller'].replace({np.NaN: None}).astype("Int64")

# Merge de ot e id_tipo_ot
df_ot = pd.merge(df_ot, df_tipo_ot, how='left', on='tipo_ot')
# Eliminar columna tipo_ot
df_ot = df_ot.drop('tipo_ot', axis=1)

# Merge de ot e id_frecuencia
df_ot = pd.merge(df_ot, df_frecuencia, how='left', on='frecuencia')
# Eliminar columna frecuencia
df_ot = df_ot.drop('frecuencia', axis=1)

# Merge de ot e id_mecanico
df_ot = pd.merge(df_ot, df_personal, how='left', left_on='mecanico', right_on='nombre')
# Eliminar columna mecanico
df_ot = df_ot.drop('mecanico', axis=1)
# Como salen Nan hay que pasarlo a int
df_ot['mecanico'] = df_ot['mecanico'].replace({np.NaN: None}).astype("Int64")

# Se reordenan las columnas
df_ot = df_ot[['id_ot', 'ot', 'camion', 'id_tipo_ot', 'id_frecuencia', 'id_taller',
               'id_mecanico', 'fecha_inicio', 'fecha_fin', 'descripcion_trabajo_solicitado',
               'descripcion_trabajo_realizado', 'observacion']]


# ################ OT AVERIA ################

# Se crea la lista de averias
df_tipo_averia = df_averia.drop('id_averia', axis=1)
df_tipo_averia['averia'] = df_tipo_averia['averia'].str.lower()
averias = df_tipo_averia['averia'].tolist()

# Se crea la tabla con el ot y las averias
df_otc_averia = df_ot_averia.rename(columns={'NºOTC': 'ot'}).dropna(subset='ot')

# Se juntan e invierten columnas
df_ot_averia = pd.melt(
    df_otc_averia,
    id_vars='ot',
    value_vars=averias
).dropna(subset='value').loc[:, ['ot', 'variable']].rename(columns={'variable': 'averia'})

# Se limpian los datos
df_ot_averia['averia'] = limpiar_capitalize(df_ot_averia, 'averia')

# Merge ot
df_ot_averia = pd.merge(df_ot_averia, df_ot.loc[:, ['id_ot', 'ot']], how='left', on='ot')
df_ot_averia.dropna(subset=['id_ot'], inplace=True)  # NO ES NECESARIO CREO
df_ot_averia['id_ot'] = df_ot_averia['id_ot'].astype(int)  # NO ES NECESARIO CREO
# Se elimina la columna ot
df_ot_averia = df_ot_averia.drop('ot', axis=1)

# Merge averia
df_ot_averia = pd.merge(df_ot_averia, df_averia, how='left', on='averia')
# Se elimina la columna averia
df_ot_averia = df_ot_averia.drop('averia', axis=1)

# ################ OT REPUESTO ################

#Se crea la lista de repuestos
df_tipo_repuesto = df_repuesto.drop('id_repuesto', axis=1)
df_tipo_repuesto['repuesto'] = df_tipo_repuesto['repuesto'].str.lower()

# Creamos una lista con los repuesto a quitar
nombres_a_quitar = ['filtro de aceite', 'filtro de aire', 'filtro de gasoil',
'filtro hidraulico', 'filtro separador de gasoil', 'pre-filtro de gasoil']

# Seleccionamos las filas que contienen los nombres a eliminar
filas_a_eliminar = df_tipo_repuesto.loc[df_tipo_repuesto['repuesto'].isin(nombres_a_quitar)]

# Eliminamos las filas seleccionadas
df_tipo_repuesto = df_tipo_repuesto.drop(filas_a_eliminar.index)

repuestos = df_tipo_repuesto['repuesto'].tolist()

#Se crea la tabla con el ot y los repuestos
df_otc_repuesto = df_ot_repuesto.rename(columns={'NºOTC': 'ot'}).dropna(subset='ot')

# Se juntan e invierten columnas
df_ot_repuesto = pd.melt(
    df_otc_repuesto,
    id_vars='ot',
    value_vars=repuestos
).dropna(subset='value').loc[:, ['ot', 'variable']].rename(columns={'variable': 'repuesto'})

# Se limpian los datos
df_ot_repuesto['repuesto'] = limpiar_capitalize(df_ot_repuesto, 'repuesto')

# Merge ot
df_ot_repuesto = pd.merge(df_ot_repuesto, df_ot.loc[:, ['id_ot', 'ot']], how='left', on='ot')
df_ot_repuesto.dropna(subset=['id_ot'], inplace=True)  # NO ES NECESARIO CREO
# Se elimina la columna ot
df_ot_repuesto = df_ot_repuesto.drop('ot', axis=1)
df_ot_repuesto['id_ot'] = df_ot_repuesto['id_ot'].astype(int)  # NO ES NECESARIO CREO

# Merge repuesto
df_ot_repuesto = pd.merge(df_ot_repuesto, df_repuesto, how='left', on='repuesto')
# Se elimina la columna repuesto
df_ot_repuesto = df_ot_repuesto.drop('repuesto', axis=1)


# ################ DBEAVER ################

# df_ot.to_sql(name='tbl_ot', con=engine, if_exists='append', index=False)
# df_ot_averia.to_sql(name='tbl_ot_averia', con=engine, if_exists='append', index=False)
# df_ot_repuesto.to_sql(name='tbl_ot_repuesto', con=engine, if_exists='append', index=False)

logger.info('Fin ETL Distribucion')
