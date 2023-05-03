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
from models.model import Wilaya

path_input = dp.rootFolder / 'data' / 'raw'
path_output = dp.rootFolder / 'data' / 'processed'
path_2023 = dp.rootFolder / 'data' / '2023'


logger = logger()

logger.info('Inicio ETL Agua')

# etl_agua()

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
ot_ultima_fecha = session.query(Ot.ot).filter(Ot.fecha_inicio == ultima_fecha).all()


# ############################################################### FALTA ELIMINAR LAS FILAS DONDE LA FECHA Y OT COINCIDEN CON LA BASE DE DATOS
# PARA ESO ACTUALIZAR DATOS HISTORICOS DE LA BASE DE DATOS (HE PASADO LAS OT A STR, ASI DEBERIA FUNCIONAR) SINO PASAR TODOS A INT Y LUEGO A STR

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


def limpiar_title(dataframe, columna):
    """
    Limpia el texto.

    Quita los espacios de adelante y atras y lo pasa
    a title.
    """
    cadena_title = dataframe[columna].str.strip().str.title()
    return cadena_title


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


def pasar_a_int(dataframe, columna):
    """
    Pasar una columna con Nan a int.
    """
    if dataframe[columna].empty:
        return dataframe[columna]
    else:
        # Reemplazar espacios por None
        dataframe[columna] = dataframe[columna].replace(r'^\s*$', np.nan, regex=True)
        dataframe[columna] = dataframe[columna].replace({np.nan: None})
        # Convertir a tipo Int64
        return dataframe[columna].astype("Int64")


# ------------------------------------------- AGUA -------------------------------------------
# ######################################### PREVENTIVO #########################################
# NO HAY COLUMNA DE MECANICOS
# SE PONE EL COSTE? OJO CON LAS FECHAS!

# Se extrae la información del excel de ot preventivo
df_prev_agua = pd.read_excel(
    path_2023 / 'BASE DE DATOS 2023 UNHCR.xlsx',
    sheet_name='Base de datos Preventiva',
    usecols='A, B, G, H, I:J, L:Z, AA',
    skiprows=1,
    parse_dates=['fecha_inicio', 'fecha_fin'],
    names=['ot', 'camion', 'taller', 'frecuencia', 'fecha_inicio', 'fecha_fin',
            'aceite motor', 'anticongelante', 'liquido de embrague',
            'liquido direccion', 'liquido de freno', 'agua destilada',
            'aceite caja cambios', 'acido baterias', 'grasa', 'filtro de aceite',
            'filtro de aire', 'filtro de gasoil', 'filtro hidraulico',
            'filtro separador de gasoil', 'pre-filtro de gasoil',
            'coste mantenimiento']
)

# Seleccionar las filas que tienen fecha posterior a la fecha de referencia
# Convertir la fecha a tipo datetime64[ns]
ultima_fecha = np.datetime64(ultima_fecha)

df_ot_agua_prev = df_prev_agua.loc[df_prev_agua['fecha_inicio'] >= ultima_fecha] 

# Se eliminan las filas con valores nan en todas las columnas
df_ot_agua_prev.dropna(how='all', inplace=True)
# si no tiene numero de ot tampoco tiene informacion
df_ot_agua_prev.dropna(subset=['ot'], inplace=True)
# camiones que son vacios y su informacion tambien
df_ot_agua_prev.dropna(subset=['camion'], inplace=True)
# SE QUITAN LOS DATOS QUE NO TIENEN FECHA DE INICIO
df_ot_agua_prev.dropna(subset=['fecha_inicio'], inplace=True)


# Se cambian los nan por un tipo de frecuencia 'No frecuencia'
df_ot_agua_prev['frecuencia'] = df_ot_agua_prev['frecuencia'].fillna('No frecuencia')

# ---------------------------------------------
# Se limpian los datos
df_ot_agua_prev['ot'] = df_ot_agua_prev['ot'].astype(str)
df_ot_agua_prev['ot'] = limpiar_upper(df_ot_agua_prev, 'ot')

df_ot_agua_prev['camion'] = limpiar_upper(df_ot_agua_prev, 'camion')

df_ot_agua_prev['taller'] = limpiar_upper(df_ot_agua_prev, 'taller')

df_ot_agua_prev['frecuencia'] = limpiar_capitalize(df_ot_agua_prev, 'frecuencia')

# df_ot_agua_prev['coste'] = df_ot_agua_prev['coste'].astype(float)

columnas_litros = ['aceite motor', 'anticongelante', 'liquido de embrague',
                   'liquido direccion', 'liquido de freno', 'agua destilada', 'aceite caja cambios',
                   'acido baterias']
df_ot_agua_prev['aceite motor'] = pasar_a_int(df_ot_agua_prev, 'aceite motor')
df_ot_agua_prev['anticongelante'] = pasar_a_int(df_ot_agua_prev, 'anticongelante')
df_ot_agua_prev['liquido de embrague'] = pasar_a_int(df_ot_agua_prev, 'liquido de embrague')
df_ot_agua_prev['liquido direccion'] = pasar_a_int(df_ot_agua_prev, 'liquido direccion')
df_ot_agua_prev['liquido de freno'] = pasar_a_int(df_ot_agua_prev, 'liquido de freno')
df_ot_agua_prev['agua destilada'] = pasar_a_int(df_ot_agua_prev, 'agua destilada')
df_ot_agua_prev['aceite caja cambios'] = pasar_a_int(df_ot_agua_prev, 'aceite caja cambios')
df_ot_agua_prev['acido baterias'] = pasar_a_int(df_ot_agua_prev, 'acido baterias')

columnas_kilos = ['grasa']
df_ot_agua_prev['grasa'] = pasar_a_int(df_ot_agua_prev, 'grasa')

columnas_unidades = ['filtro de aceite', 'filtro de aire', 'filtro de gasoil',
                     'filtro hidraulico', 'filtro separador de gasoil', 'pre-filtro de gasoil',]
df_ot_agua_prev['filtro de aceite'] = pasar_a_int(df_ot_agua_prev, 'filtro de aceite')
df_ot_agua_prev['filtro de aire'] = pasar_a_int(df_ot_agua_prev, 'filtro de aire')
df_ot_agua_prev['filtro de gasoil'] = pasar_a_int(df_ot_agua_prev, 'filtro de gasoil')
df_ot_agua_prev['filtro hidraulico'] = pasar_a_int(df_ot_agua_prev, 'filtro hidraulico')
df_ot_agua_prev['filtro separador de gasoil'] = pasar_a_int(df_ot_agua_prev, 'filtro separador de gasoil')
df_ot_agua_prev['pre-filtro de gasoil'] = pasar_a_int(df_ot_agua_prev, 'pre-filtro de gasoil')

# ---------------------------------------------
# Se reemplazan los valores de taller
df_ot_agua_prev = df_ot_agua_prev.replace({'taller': {'ATC RABUNI': 'CLM', 'TR AAIUN': 'Aaiun',
                                            'TR AUSERD': 'Auserd', 'TR BOUJDUR': 'Bojador',
                                            'TR DAJLA': 'Dajla', 'TR SMARA': 'Smara',
                                            'TR TRANSPORT': 'BDT'}})

# Se resetea el index
df_ot_agua_prev.reset_index(inplace=True, drop=True)

# ---------------------------------------------

# Crear una columna de mecanico
df_ot_agua_prev['mecanico'] = ''

# Crear una columna de tipo_ot = 'Preventivo'
df_ot_agua_prev['tipo_ot'] = 'Preventivo'

# Crear una columna de descripcion_trabajo_solicitado
df_ot_agua_prev['descripcion_trabajo_solicitado'] = ''

# Crear una columna de descripcion_trabajo_realizado
df_ot_agua_prev['descripcion_trabajo_realizado'] = ''

# Crear una columna de observacion
df_ot_agua_prev['observacion'] = ''
# ---------------------------------------------

# Se define el dataframe de ot repuesto (se usara mas adelante)
df_ot_agua_repuesto = df_ot_agua_prev[['ot', 'aceite motor', 'anticongelante', 'liquido de embrague',
                             'liquido direccion', 'liquido de freno', 'agua destilada',
                             'aceite caja cambios', 'acido baterias', 'grasa', 'filtro de aceite',
                             'filtro de aire', 'filtro de gasoil', 'filtro hidraulico',
                             'filtro separador de gasoil', 'pre-filtro de gasoil']]

# Se toman las columnas deseadas
df_ot_agua_prev = df_ot_agua_prev[['ot', 'camion', 'tipo_ot', 'frecuencia', 'taller',
                         'mecanico', 'fecha_inicio', 'fecha_fin',
                         'descripcion_trabajo_solicitado',
                         'descripcion_trabajo_realizado', 'observacion']]


# ######################################### CORRECTIVO #########################################
# PROBLEMA CON LOS MECANICOS, VA A VER SOLO ESAS AVERIAS?

# Se extrae la información del excel de ot correctivo
df_corr_agua = pd.read_excel(
    path_2023 / 'BASE DE DATOS 2023 UNHCR.xlsx',
    sheet_name='Base de datos Correctiva',
    usecols='A, B, C, E, I:J, L:P, Q, R, S',
    skiprows=10,
    parse_dates=['fecha_inicio', 'fecha_fin'],
    names=['camion', 'taller', 'ot', 'mecanico', 'fecha_inicio', 'fecha_fin', 'chasis',
            'carroceria', 'ruedas', 'mecanica', 'elec, vehiculos',
            'descripcion_trabajo_solicitado', 'descripcion_trabajo_realizado', 'observacion']
)

# Seleccionar las filas que tienen fecha igual o posterior a la fecha de referencia
df_ot_agua_corr = df_corr_agua.loc[df_corr_agua['fecha_inicio'] >= ultima_fecha]

# Se eliminan las filas con valores nan en todas las columnas
df_ot_agua_corr.dropna(how='all', inplace=True)
# si no tiene numero de ot tampoco tiene informacion
df_ot_agua_corr.dropna(subset=['ot'], inplace=True)
# camiones que son vacios y su informacion tambien
df_ot_agua_corr.dropna(subset=['camion'], inplace=True)
# SE QUITAN LOS DATOS QUE NO TIENEN FECHA DE INICIO
df_ot_agua_corr.dropna(subset=['fecha_inicio'], inplace=True)

# ---------------------------------------------
# Se limpian los datos
df_ot_agua_corr['ot'] = df_ot_agua_corr['ot'].astype(str)
df_ot_agua_corr['ot'] = limpiar_upper(df_ot_agua_corr, 'ot')

df_ot_agua_corr['camion'] = limpiar_upper(df_ot_agua_corr, 'camion')

df_ot_agua_corr['mecanico'] = limpiar_capitalize(df_ot_agua_corr, 'mecanico')

df_ot_agua_corr['descripcion_trabajo_solicitado'] = limpiar_capitalize(df_ot_agua_corr, 'descripcion_trabajo_solicitado')

df_ot_agua_corr['descripcion_trabajo_realizado'] = limpiar_capitalize(df_ot_agua_corr, 'descripcion_trabajo_realizado')

df_ot_agua_corr['observacion'] = limpiar_capitalize(df_ot_agua_corr, 'observacion')

columnas_averias = ['chasis', 'carroceria', 'ruedas', 'mecanica', 'elec, vehiculos']
df_ot_agua_corr['chasis'] = pasar_a_int(df_ot_agua_corr, 'chasis')
df_ot_agua_corr['carroceria'] = pasar_a_int(df_ot_agua_corr, 'carroceria')
df_ot_agua_corr['ruedas'] = pasar_a_int(df_ot_agua_corr, 'ruedas')
df_ot_agua_corr['mecanica'] = pasar_a_int(df_ot_agua_corr, 'mecanica')
df_ot_agua_corr['elec, vehiculos'] = pasar_a_int(df_ot_agua_corr, 'elec, vehiculos')

# ---------------------------------------------
# Se reemplazan los valores de taller
df_ot_agua_corr = df_ot_agua_corr.replace({'taller': {'ATC RABUNI': 'CLM', 'TR AAIUN': 'Aaiun',
                                            'TR AUSERD': 'Auserd', 'TR BOUJDUR': 'Bojador',
                                            'TR DAJLA': 'Dajla', 'TR SMARA': 'Smara',
                                            'TR TRANSPORT': 'BDT'}})

# ---------------------------------------------
# Crear una columna de frecuencia = 'No frecuencia'
df_ot_agua_corr['frecuencia'] = 'No frecuencia'

# Crear una columna de tipo_ot = 'Correctivo'
df_ot_agua_corr['tipo_ot'] = 'Correctivo'

# REVISAR LOS MECANICOS CAMBIADOS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Se reemplazan los nombres de los mecanicos
df_ot_agua_corr = df_ot_agua_corr.replace({'mecanico': {'Saleh': 'Saleh Mohamed Salma',
                                              'Mohamed lamin': 'Mohamed Lamin Hussein',
                                              'Bal-lal hammu': 'Bel-lal Hammu',
                                              'Ahmed salek andal': 'Ahmed Salec',
                                              'Salek ali': 'Salec Ali',
                                              'Ali': 'Ali Eluali',
                                              'Mohamed abdalahi': 'Mohamed-Abdalahe Mohamed-Bachir',
                                              'Laulad': 'Laulad Mohamed-Salem',
                                              'Sidi alal': 'Sidi alal Brahim',
                                              'Jatri mohamed': 'Jatri  Mohamed',
                                              'Todos': None,
                                              'Mec transporte ': None,
                                              'Conductor': None,
                                              'Lamni': None,
                                              'Mohamed': None,
                                              'Moh lamin': 'Mohamed Lamin Mustafa',
                                              'Ali salem': 'Alisalem Mohamed-Salem'}})

# ---------------------------------------------

# Se resetea el index por los valores eliminados
df_ot_agua_corr.reset_index(inplace=True, drop=True)

# Se define el dataframe de ot averia (se usara mas adelante)
df_ot_agua_averia = df_ot_agua_corr[['ot', 'chasis', 'carroceria', 'ruedas',
                           'mecanica', 'elec, vehiculos']]

# Se toman las columnas deseadas
df_ot_agua_corr = df_ot_agua_corr[['ot', 'camion', 'tipo_ot', 'frecuencia', 'taller',
                         'mecanico', 'fecha_inicio', 'fecha_fin',
                         'descripcion_trabajo_solicitado',
                         'descripcion_trabajo_realizado', 'observacion']]


# ######################################### UNION #########################################

# Concat de ambos dataframes
df_ot_agua = pd.concat([df_ot_agua_corr, df_ot_agua_prev], axis=0)

# Se toman las columnas deseadas
df_ot_agua = df_ot_agua[['ot', 'camion', 'tipo_ot', 'frecuencia', 'taller', 'mecanico',
               'fecha_inicio', 'fecha_fin', 'descripcion_trabajo_solicitado',
               'descripcion_trabajo_realizado', 'observacion']]

# Se quitan los indices anteriores ya que se repiten cada anho
df_ot_agua.reset_index(inplace=True, drop=True)

# ######################################### MERGES #########################################

# Merge de ot e id_camion
df_ot_agua = pd.merge(df_ot_agua, df_camion.loc[:, ['id_camion', 'nombre_attsf']], how='left', left_on='camion', right_on='nombre_attsf')
# Eliminar columna nombre_attsf
df_ot_agua = df_ot_agua.drop('nombre_attsf', axis=1)

# ---------------TALLER VACIO ---------------
# Como hay talleres vacios se extraen del camion y las wilayas
df_ot_agua = pd.merge(df_ot_agua, df_camion.loc[:, ['nombre_attsf', 'id_wilaya', 'id_tipo_vehiculo']],
                 how='left', left_on='camion', right_on='nombre_attsf')
df_ot_agua = pd.merge(df_ot_agua, df_wilaya.loc[:, ['id_wilaya', 'wilaya']], how='left', on='id_wilaya')
# Eliminar columna id_wilaya, camion, nombre_attsf
df_ot_agua = df_ot_agua.drop(['id_wilaya', 'camion', 'nombre_attsf'], axis=1)

# Se crea una columna taller y se le aplica la funcion
df_ot_agua['taller2'] = ''
df_ot_agua['taller2'] = df_ot_agua.apply(asociar_wilaya_taller, axis=1)

# Si hay valores nan en taller sustituye dichos valores por los obtenidos con la funcion
df_ot_agua['taller'] = df_ot_agua['taller'].replace('', np.NaN)
df_ot_agua['taller'] = df_ot_agua['taller'].fillna(df_ot_agua['taller2'])

# Eliminar columna wilaya, id_tipo_vehiculo, taller2
df_ot_agua = df_ot_agua.drop(['wilaya', 'id_tipo_vehiculo', 'taller2'], axis=1)
# ---------------------------------------------

# Merge de ot e id_taller
df_ot_agua = pd.merge(df_ot_agua, df_taller.loc[:, ['id_taller', 'taller']], how='left', on='taller')
# Eliminar columna taller
df_ot_agua = df_ot_agua.drop('taller', axis=1)
# Como salen Nan hay que pasarlo a int
df_ot_agua['id_taller'] = df_ot_agua['id_taller'].replace({np.NaN: None}).astype("Int64")

# Merge de ot e id_tipo_ot
df_ot_agua = pd.merge(df_ot_agua, df_tipo_ot, how='left', on='tipo_ot')
# Eliminar columna tipo_ot
df_ot_agua = df_ot_agua.drop('tipo_ot', axis=1)

# Merge de ot e id_frecuencia
df_ot_agua = pd.merge(df_ot_agua, df_frecuencia, how='left', on='frecuencia')
# Eliminar columna frecuencia
df_ot_agua = df_ot_agua.drop('frecuencia', axis=1)

# Merge de ot e id_mecanico
df_ot_agua = pd.merge(df_ot_agua, df_personal.loc[:, ['id_personal', 'nombre']], how='left', left_on='mecanico', right_on='nombre')
# Cambiar nombre de la columna id_personal
df_ot_agua = df_ot_agua.rename(columns={'id_personal': 'id_mecanico'})
# Eliminar columna mecanico
df_ot_agua = df_ot_agua.drop('mecanico', axis=1)
# Como salen Nan hay que pasarlo a int
df_ot_agua['id_mecanico'] = df_ot_agua['id_mecanico'].replace({np.NaN: None}).astype("Int64")

# Se reordenan las columnas
df_ot_agua = df_ot_agua[['ot', 'id_camion', 'id_tipo_ot', 'id_frecuencia', 'id_taller',
               'id_mecanico', 'fecha_inicio', 'fecha_fin', 'descripcion_trabajo_solicitado',
               'descripcion_trabajo_realizado', 'observacion']]

# ######################################### OT COMPROBAR FECHA #########################################

# Se elimina la fila cuya ot es ultima_ot y fecha es ultima_fecha
df_ot_agua = df_ot_agua.drop(df_ot_agua[(df_ot_agua['fecha_inicio'] == ultima_fecha) & (df_ot_agua['ot'].isin(ot_ultima_fecha[0]))].index)

# Ordenar por fecha
df_ot_agua = df_ot_agua.sort_values(by='fecha_inicio')

df_ot_agua.reset_index(inplace=True, drop=True)

# Numero de filas del dataframe
num_filas = df_ot_agua.shape[0]

# Generar una Serie desde el numero ultima_id_ot
num_ot = pd.Series(range(ultima_id_ot + 1, num_filas + ultima_id_ot + 1))



# Se reordenan las columnas
df_ot_agua = df_ot_agua[['id_ot', 'ot', 'id_camion', 'id_tipo_ot', 'id_frecuencia', 'id_taller',
               'id_mecanico', 'fecha_inicio', 'fecha_fin', 'descripcion_trabajo_solicitado',
               'descripcion_trabajo_realizado', 'observacion']]

# ######################################### OT AVERIA #########################################

# Se crea la lista de averias
df_tipo_averia = df_averia.drop('id_averia', axis=1)
df_tipo_averia['averia'] = df_tipo_averia['averia'].str.lower()

# ------------------ Quitar averias ---------------------------
# Creamos una lista con los repuesto a quitar
nombres_a_quitar = ['obra civil', 'agua y combustible', 'herramientas',
                    'informatica', 'exteriores', 'aire', 'maquinaria', 'electricidad']
# Seleccionamos las filas que contienen los nombres a eliminar
filas_a_eliminar = df_tipo_averia.loc[df_tipo_averia['averia'].isin(nombres_a_quitar)]
# Eliminamos las filas seleccionadas
df_tipo_averia = df_tipo_averia.drop(filas_a_eliminar.index)
# -------------------------------------------------

averias = df_tipo_averia['averia'].tolist()

# Se crea la tabla con el ot y las averias
df_ot_aguac_averia = df_ot_agua_averia.rename(columns={'NºOTC': 'ot'}).dropna(subset='ot')

# Se juntan e invierten columnas
df_ot_agua_averia = pd.melt(
    df_ot_aguac_averia,
    id_vars='ot',
    value_vars=averias
).dropna(subset='value').loc[:, ['ot', 'variable']].rename(columns={'variable': 'averia'})

# Se limpian los datos
df_ot_agua_averia['averia'] = limpiar_capitalize(df_ot_agua_averia, 'averia')

# Merge ot
df_ot_agua_averia = pd.merge(df_ot_agua_averia, df_ot_agua.loc[:, ['id_ot', 'ot']], how='left', on='ot')
df_ot_agua_averia.dropna(subset=['id_ot'], inplace=True)  # NO ES NECESARIO CREO
df_ot_agua_averia['id_ot'] = df_ot_agua_averia['id_ot'].astype(int)  # NO ES NECESARIO CREO
# Se elimina la columna ot
df_ot_agua_averia = df_ot_agua_averia.drop('ot', axis=1)

# Merge averia
df_ot_agua_averia = pd.merge(df_ot_agua_averia, df_averia, how='left', on='averia')
# Se elimina la columna averia
df_ot_agua_averia = df_ot_agua_averia.drop('averia', axis=1)

# ######################################### OT REPUESTO #########################################

# Se crea la lista de repuestos
df_tipo_repuesto = df_repuesto.drop('id_repuesto', axis=1)
df_tipo_repuesto['repuesto'] = df_tipo_repuesto['repuesto'].str.lower()

# Creamos una lista con los repuesto a quitar
nombres_a_quitar = ['ruedas', 'lamparas', 'd/tacgfo', 's. lava', 'j. carroceria', 'calcho']

# Seleccionamos las filas que contienen los nombres a eliminar
filas_a_eliminar = df_tipo_repuesto.loc[df_tipo_repuesto['repuesto'].isin(nombres_a_quitar)]

# Eliminamos las filas seleccionadas
df_tipo_repuesto = df_tipo_repuesto.drop(filas_a_eliminar.index)

repuestos = df_tipo_repuesto['repuesto'].tolist()

# Se crea la tabla con el ot y los repuestos
df_ot_aguac_repuesto = df_ot_agua_repuesto.rename(columns={'NºOTC': 'ot'}).dropna(subset='ot')

# Se juntan e invierten columnas
df_ot_agua_repuesto = pd.melt(
    df_ot_aguac_repuesto,
    id_vars='ot',
    value_vars=repuestos
).dropna(subset='value').loc[:, ['ot', 'variable']].rename(columns={'variable': 'repuesto'})

# Se limpian los datos
df_ot_agua_repuesto['repuesto'] = limpiar_capitalize(df_ot_agua_repuesto, 'repuesto')

# Merge ot
df_ot_agua_repuesto = pd.merge(df_ot_agua_repuesto, df_ot_agua.loc[:, ['id_ot', 'ot']], how='left', on='ot')
df_ot_agua_repuesto.dropna(subset=['id_ot'], inplace=True)  # NO ES NECESARIO CREO
# Se elimina la columna ot
df_ot_agua_repuesto = df_ot_agua_repuesto.drop('ot', axis=1)
df_ot_agua_repuesto['id_ot'] = df_ot_agua_repuesto['id_ot'].astype(int)  # NO ES NECESARIO CREO

# Merge repuesto
df_ot_agua_repuesto = pd.merge(df_ot_agua_repuesto, df_repuesto, how='left', on='repuesto')
# Se elimina la columna repuesto
df_ot_agua_repuesto = df_ot_agua_repuesto.drop('repuesto', axis=1)







df_ot_agua.to_csv(path_output / "BORRAR2.csv", index=False, encoding='utf-8')


# ######################################### DBEAVER #########################################

# df_ot_agua.to_sql('tbl_ot', con=engine, if_exists='append', index=False)
# df_ot_agua_averia.to_sql('tbl_ot_averia', con=engine, if_exists='append', index=False)
# df_ot_agua_repuesto.to_sql('tbl_ot_repuesto', con=engine, if_exists='append', index=False)

session.close()
logger.info('Fin ETL Agua')