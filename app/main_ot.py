import pandas as pd
import numpy as np
import re
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
df_wilaya = pd.read_sql(session.query(Wilaya).statement, conn)
# Obtener la ultima fecha, el ultimo id_ot y los ots que tienen la ultima fecha
ultima_fecha = session.query(Ot.fecha_inicio).filter(Ot.fecha_inicio != None).order_by(Ot.fecha_inicio.desc()).first()[0]
ultima_id_ot = session.query(Ot.id_ot).order_by(Ot.id_ot.desc()).first()[0]
ot_ultima_fecha = session.query(Ot.ot).filter(Ot.fecha_inicio == ultima_fecha)


# FUNCIONES
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


def asociar_wilaya_taller(fila):
    """
    Asocia un taller a una ot.

    A partir de la wilaya asociada a un camion
    asocia el taller correspondiente.
    """
    if ((fila['wilaya'] != 'No Wilaya') and (fila['wilaya'] != 'Rabouni')):
        return fila['wilaya']
    elif ((fila['wilaya'] == 'No Wilaya') or (fila['wilaya'] == 'Rabouni' and fila['id_tipo_vehiculo'] == 3)):
        return 'BDT'
    else:
        return 'CLM'


# Funcion para reemplazarlo por CA
def replace_camion(match):
    return f"CA{match.group(1)}"


# ------------------------------------------- ALIMENTOS -------------------------------------------
# ######################################### PREVENTIVO #########################################
# MECANICOS?????????? OJO CON LAS FECHAS!

# Se extrae la información del excel de ot preventivo
df_prev = pd.read_excel(
    path_input / 'OTP DE 2023 NUEVA.xlsx',
    sheet_name='OTP2022',
    usecols='A, C, D, E, I, K, P:AD, AE, AH',
    skiprows=6,
    parse_dates=['fecha_inicio', 'fecha_fin'],
    names=['camion', 'ot', 'frecuencia', 'mecanico', 'fecha_inicio', 'fecha_fin',
            'aceite motor', 'anticongelante', 'liquido de embrague',
            'liquido direccion', 'acido baterias', 'liquido de freno', 'aceite caja cambios',
            'agua destilada', 'ruedas', 'lamparas', 'd/tacgfo', 's. lava',
            'j. carroceria', 'calcho', 'grasa', 'descripcion_trabajo_solicitado',
            'observacion'])

print(df_prev)
# Seleccionar las filas que tienen fecha posterior a la fecha de referencia
df_ot_prev = df_prev.loc[df_prev['fecha_inicio'] >= ultima_fecha]

# Se eliminan las filas con valores nan en todas las columnas
df_ot_prev.dropna(how='all', inplace=True)
# si no tiene numero de ot tampoco tiene informacion
df_ot_prev.dropna(subset=['ot'], inplace=True)
# camiones que son vacios y su informacion tambien
df_ot_prev.dropna(subset=['camion'], inplace=True)
# SE QUITAN LOS DATOS QUE NO TIENEN FECHA DE INICIO
df_ot_prev.dropna(subset=['fecha_inicio'], inplace=True)


# Se cambian los nan por un tipo de frecuencia 'No frecuencia'
df_ot_prev['frecuencia'] = df_ot_prev['frecuencia'].fillna('No frecuencia')

# ---------------------------------------------
# Se limpian los datos
df_ot_prev['ot'] = df_ot_prev['ot'].astype(str)
df_ot_prev['ot'] = limpiar_upper(df_ot_prev, 'ot')

df_ot_prev['camion'] = limpiar_upper(df_ot_prev, 'camion')

df_ot_prev['mecanico'] = limpiar_capitalize(df_ot_prev, 'mecanico')

df_ot_prev['frecuencia'] = limpiar_capitalize(df_ot_prev, 'frecuencia')

columnas_repuestos = ['aceite motor', 'anticongelante', 'liquido de embrague',
                      'liquido direccion', 'acido baterias', 'liquido de freno',
                      'aceite caja cambios', 'agua destilada', 'ruedas',
                      'lamparas', 'd/tacgfo', 's. lava', 'j. carroceria',
                      'calcho', 'grasa']
df_ot_prev[columnas_repuestos] = df_ot_prev[columnas_repuestos].astype(int)

# Se quitan los duplicatos OJO
# df_ot_prev = df_ot_prev.drop_duplicates()

# ---------------------------------------------
# Se reemplazan los valores de mecanico
############ df_ot_prev = df_ot_prev.replace({'mecanico': {---------------------------------------------}})

# Se resetea el index
df_ot_prev.reset_index(inplace=True, drop=True)

# ---------------TALLER VACIO ---------------
# Como hay talleres vacios se extraen del camion y las wilayas
df_ot_prev = pd.merge(df_ot_prev, df_camion.loc[:, ['id_camion', 'nombre_attsf', 'id_wilaya', 'id_tipo_vehiculo']],
                      how='left', left_on='camion', right_on='nombre_attsf')
df_ot_prev = pd.merge(df_ot_prev, df_wilaya.loc[:, ['id_wilaya', 'wilaya']], how='left', on='id_wilaya')
# Eliminar columna id_wilaya, id_camion, nombre_attsf
df_ot_prev = df_ot_prev.drop(['id_wilaya', 'id_camion', 'nombre_attsf'], axis=1)

# Se crea una columna taller y se le aplica la funcion
df_ot_prev['taller'] = ''
df_ot_prev['taller'] = df_ot_prev.apply(asociar_wilaya_taller, axis=1)

# Eliminar columna wilaya, id_tipo_vehiculo, taller2
df_ot_prev = df_ot_prev.drop(['wilaya', 'id_tipo_vehiculo'], axis=1)

# ---------------------------------------------

# Crear una columna de tipo_ot = 'Preventivo'
df_ot_prev['tipo_ot'] = 'Preventivo'

# Crear una columna de descripcion_trabajo_realizado
df_ot_prev['descripcion_trabajo_realizado'] = ''

# ---------------------------------------------

# Se define el dataframe de ot repuesto (se usara mas adelante)
df_ot_repuesto = df_ot_prev[['ot', 'aceite motor', 'anticongelante', 'liquido de embrague',
                            'liquido direccion', 'acido baterias', 'liquido de freno',
                            'aceite caja cambios', 'agua destilada', 'ruedas', 
                            'lamparas', 'd/tacgfo', 's. lava', 'j. carroceria', 
                            'calcho', 'grasa']]

# Se toman las columnas deseadas
df_ot_prev = df_ot_prev[['ot', 'camion', 'tipo_ot', 'frecuencia', 'taller',
                         'mecanico', 'fecha_inicio', 'fecha_fin',
                         'descripcion_trabajo_solicitado',
                         'descripcion_trabajo_realizado' 'observacion']]


# ######################################### CORRECTIVO #########################################
# PROBLEMA CON LOS MECANICOS, VA A VER SOLO ESAS AVERIAS?

# Se extrae la información del excel de ot correctivo
df_corr = pd.read_excel(
    path_input / 'Base de Trabajo correctivas  de 2023.xlsx',
    sheet_name='Base de datos Correctiva',
    usecols='A, C, E, I, K, R:AD, AE, AF, AG',
    skiprows=5,
    parse_dates=['fecha_inicio', 'fecha_fin'],
    names=['camion', 'ot', 'mecanico', 'fecha_inicio', 'fecha_fin', 'chasis',
            'carroceria', 'ruedas', 'mecanica', 'elec, vehiculos', 'obra civil',
            'agua y combustible', 'herramientas', 'informatica', 'exteriores', 
            'aire', 'maquinaria', 'electricidad', 'descripcion_trabajo_solicitado',
            'descripcion_trabajo_realizado', 'observacion'])

# Seleccionar las filas que tienen fecha igual o posterior a la fecha de referencia
df_ot_corr = df_corr.loc[df_corr['fecha_inicio'] >= ultima_fecha]

# Se eliminan las filas con valores nan en todas las columnas
df_ot_corr.dropna(how='all', inplace=True)
# si no tiene numero de ot tampoco tiene informacion
df_ot_corr.dropna(subset=['ot'], inplace=True)
# camiones que son vacios y su informacion tambien
df_ot_corr.dropna(subset=['camion'], inplace=True)
# SE QUITAN LOS DATOS QUE NO TIENEN FECHA DE INICIO
df_ot_corr.dropna(subset=['fecha_inicio'], inplace=True)

# ---------------------------------------------
# Se limpian los datos
df_ot_corr['ot'] = df_ot_corr['ot'].astype(str)
df_ot_corr['ot'] = limpiar_upper(df_ot_corr, 'ot')

df_ot_corr['camion'] = limpiar_upper(df_ot_corr, 'camion')

df_ot_corr['mecanico'] = limpiar_capitalize(df_ot_corr, 'mecanico')

df_ot_corr['taller'] = ''

df_ot_corr['descripcion_trabajo_solicitado'] = limpiar_capitalize(df_ot_corr, 'descripcion_trabajo_solicitado')

df_ot_corr['descripcion_trabajo_realizado'] = limpiar_capitalize(df_ot_corr, 'descripcion_trabajo_realizado')

df_ot_corr['observacion'] = limpiar_capitalize(df_ot_corr, 'observacion')

columnas_averias = ['chasis', 'carroceria', 'ruedas', 'elec, vehiculos', 'obra civil',
                    'agua y combustible', 'herramientas', 'informatica', 'exteriores',
                    'aire', 'maquinaria']
df_ot_corr[columnas_averias] = df_ot_corr[columnas_averias].astype(int)

# Eliminar los duplicados OJO
# df_ot_corr = df_ot_corr.drop_duplicates()

# ---------------------------------------------
# Se reemplazan los valores de mecanico
############### df_ot_corr = df_ot_corr.replace({'mecanico': {------------------------------------}})

# Se resetea el index por los valores eliminados
df_ot_corr.reset_index(inplace=True, drop=True)

# ---------------------------------------------
# Crear una columna de frecuencia = 'No frecuencia'
df_ot_corr['frecuencia'] = 'No frecuencia'

# Crear una columna de tipo_ot = 'Correctivo'
df_ot_corr['tipo_ot'] = 'Correctivo'

# ---------------------------------------------
# Se define el dataframe de ot averia (se usara mas adelante)
df_ot_averia = df_ot_corr[['ot', 'chasis', 'carroceria', 'ruedas', 'elec, vehiculos',
                           'obra civil', 'agua y combustible', 'herramientas',
                           'informatica', 'exteriores', 'aire', 'maquinaria']]

# Se toman las columnas deseadas
df_ot_corr = df_ot_corr[['ot', 'camion', 'tipo_ot', 'frecuencia', 'taller',
                         'mecanico', 'fecha_inicio', 'fecha_fin',
                         'descripcion_trabajo_solicitado',
                         'descripcion_trabajo_realizado', 'observacion']]


# ######################################### UNION #########################################

# Concat de ambos dataframes
df_ot = pd.concat([df_ot_corr, df_ot_prev], axis=0)

# Se toman las columnas deseadas
df_ot = df_ot[['ot', 'camion', 'tipo_ot', 'frecuencia', 'taller', 'mecanico',
               'fecha_inicio', 'fecha_fin', 'descripcion_trabajo_solicitado',
               'descripcion_trabajo_realizado', 'observacion']]

# Se quitan los indices anteriores ya que se repiten cada anho
df_ot.reset_index(inplace=True, drop=True)

# Lista de "camiones" a eliminar (en la columna camiones hay nombres que no son camiones)
nombres_a_eliminar = ['Instalaciones', 'Autorizados', 'Sierra', 'Grupo electrógeno',
'M. ruedas', 'Columna elevadora', 'Torno', 'Taladro', 'Compresor nº 2', 'Compresor nº 1',
'Prensa', 'Hidrolavadora', 'Sist. combustible', 'Sist. aire', 'Sist. agua', 
'Grupo elecrogeno', 'Otros']

# Seleccionamos las filas que contienen los nombres a eliminar
df_nocamiones = df_ot.loc[df_ot['camion'].isin(nombres_a_eliminar)]

# Eliminamos las filas seleccionadas del dataframe original
df_ot = df_ot.drop(df_nocamiones.index)

#Se resetea el index por los valores modificados
df_ot.reset_index(inplace=True, drop=True)

# ######################################### MERGES #########################################

# CAMBIAR EL CAMION NUM POR CA NUM
# Encuentra las palabras "Camión" seguidas de un número
patron = re.compile(r'CAMIÓN (\d+)')
# Aplicamos la funcion de reemplazo en la columna 'camion'
df_ot['camion'] = df_ot['camion'].str.replace(patron, replace_camion)

#Merge de ot e id_camion
df_ot = pd.merge(df_ot, df_camion.loc[:, ['id_camion', 'nombre_attsf', 'id_wilaya', 'id_tipo_vehiculo']], how='left', left_on='camion', right_on='nombre_attsf')
#Eliminar columna camion
df_ot = df_ot.drop(['camion','nombre_attsf'], axis=1)

# Para tomar los talleres se extrae la informacion de las wilayas
df_ot = pd.merge(df_ot, df_wilaya.loc[:, ['id_wilaya', 'wilaya']], how='left', on='id_wilaya')
#Eliminar columna id_wilaya
df_ot = df_ot.drop('id_wilaya', axis=1)

#Eliminar columna wilaya, id_tipo_vehiculo
df_ot = df_ot.drop(['wilaya', 'id_tipo_vehiculo'], axis=1)

# Se crea una columna taller
df_ot['taller'] = ''
df_ot['taller'] = df_ot.apply(asociar_wilaya_taller, axis=1)

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

df_ot.reset_index(inplace=True, drop=True)

# Se reordenan las columnas
df_ot = df_ot[['ot', 'camion', 'id_tipo_ot', 'id_frecuencia', 'id_taller',
               'id_mecanico', 'fecha_inicio', 'fecha_fin', 'descripcion_trabajo_solicitado',
               'descripcion_trabajo_realizado', 'observacion']]

# ######################################### OT COMPROBAR FECHA #########################################

# Se elimina la fila cuya ot es ultima_ot y fecha es ultima_fecha
df_ot = df_ot.drop(df_ot[(df_ot['fecha_inicio'] == ultima_fecha) & (df_ot['ot'].isin(ot_ultima_fecha['ot']))].index)

df_ot.reset_index(inplace=True, drop=True)

# Ordenar por fecha
df_ot = df_ot.sort_values(by='fecha_inicio')

# Numero de filas del dataframe
num_filas = df_ot.shape[0]

# Generar una Serie desde el numero ultima_id_ot
num_ot = pd.Series(range(ultima_id_ot, num_filas + ultima_id_ot))
df_ot['id_ot'] = num_ot

# Se reordenan las columnas
df_ot = df_ot[['id_ot', 'ot', 'camion', 'id_tipo_ot', 'id_frecuencia', 'id_taller',
               'id_mecanico', 'fecha_inicio', 'fecha_fin', 'descripcion_trabajo_solicitado',
               'descripcion_trabajo_realizado', 'observacion']]


# ######################################### OT AVERIA #########################################

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

# ######################################### OT REPUESTO #########################################

# Se crea la lista de repuestos
df_tipo_repuesto = df_repuesto.drop('id_repuesto', axis=1)
df_tipo_repuesto['repuesto'] = df_tipo_repuesto['repuesto'].str.lower()

# Creamos una lista con los repuesto a quitar
nombres_a_quitar = ['filtro de aceite', 'filtro de aire', 'filtro de gasoil',
                    'filtro hidraulico', 'filtro separador de gasoil', 
                    'pre-filtro de gasoil']

# Seleccionamos las filas que contienen los nombres a eliminar
filas_a_eliminar = df_tipo_repuesto.loc[df_tipo_repuesto['repuesto'].isin(nombres_a_quitar)]

# Eliminamos las filas seleccionadas
df_tipo_repuesto = df_tipo_repuesto.drop(filas_a_eliminar.index)

repuestos = df_tipo_repuesto['repuesto'].tolist()

# Se crea la tabla con el ot y los repuestos
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


# ######################################### DBEAVER #########################################

# df_ot.to_sql('tbl_ot', con=engine, if_exists='append', index=False)
# df_ot_averia.to_sql('tbl_ot_averia', con=engine, if_exists='append', index=False)
# df_ot_repuesto.to_sql('tbl_ot_repuesto', con=engine, if_exists='append', index=False)

session.close()

logger.info('Fin ETL Distribucion')
