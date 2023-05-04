# ========== Modulos Python =============
"""Librerias públiclas de python."""
import re
import pandas as pd
import numpy as np
# =========== Modulos propios ===========
import dependencies as dp


# Funcion que formatea columnas de texto
def columnas_texto(columna, formato):
    """
    Funcion que da formato a las columnas de texto (upper / title / capitalize / lower).

    Quita espación en blanco al inicio y al final.
    """
    if formato == "capitalize":
        columna = columna.str.capitalize()
    if formato == "upper":
        columna = columna.str.upper()
    if formato == "lower":
        columna = columna.str.lower()
    else:
        columna = columna.str.title()
    columna = columna.str.lstrip()
    columna = columna.str.rstrip()
    return (columna)


# Funcion que asocia un taller a un camion
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


# Funcion que pasa a int una columna con NaN
def pasar_a_int(dataframe, columna):
    """
    Para Repuestos y Averias.

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


# Funcion para reemplazar Camion por CA
def replace_camion(match):
    """
    Replace.

    Tomar los Camion y
    reemplazarlo por CA.
    """
    return f"CA{match.group(1)}"


# Funcion que busca hoja de excel
def busqueda_hoja(file_name):
    """
    Funcion que busca el nombre de una hoja de excel de acuerdo a un patron dado.

    Devuelve la cadena encontrada.
    """
    # Crear path de origen de nuevos datos
    path_input_nuevos = dp.rootFolder / 'data' / 'nuevos_datos'

    # Carga el archivo de Excel
    excel_file = pd.ExcelFile(path_input_nuevos / file_name)

    # Definimos el patrón a buscar en el nombre de la hoja
    patron = re.compile(r"base\s+datos\s+\d{4}")

    # Iteramos sobre las hojas y buscamos aquellas cuyo nombre coincida con el patrón
    for hoja in excel_file.sheet_names:
        if patron.search(hoja):
            hoja_1 = hoja

    return (hoja_1)


def elements_not_in_list(list1, list2):
    """
    Comprueba que los elementos de la lista 2 están en la lista 1. Si hay alguno
    que no está, lo retorna.

    Args:
    - list1: lista de elementos.
    - list2: lista de elementos a comprobar.

    Returns:
    - not_in_list1: lista de elementos que no se encuentran en la lista 1.
    """
    not_in_list1 = [elem for elem in list2 if elem not in list1]

    if len(not_in_list1) != 0:
        return not_in_list1


def check_relations(db_table, df, df_master, column):
    """
    Registra los elementos que tienen algún id de otra tabla mal referenciado.

    Args:
    - db_table: nombre de la tabla en la que se están comprobando las relaciones.
    - df: DataFrame de la tabla.
    - df_master: DataFrame de la tabla maestra.
    - column: columna que contiene los ids de la tabla maestra.

    Returns:
    - DataFrame filtrado sin los elementos mal referenciados.
    """
    out_elemnts = elements_not_in_list(
        df_master.iloc[:, 0].tolist(), df[column].tolist()
    )
    if out_elemnts is not None:
        df_filtered = df[df[column].isin(out_elemnts)]
        dp.logger.warning(
            f"En la tabla {db_table}, {column} mal referenciado para los siguientes elementos: "
            + ", ".join(str(id) for id in df_filtered.iloc[:, 1])
        )
        return df[~df[column].isin(out_elemnts)]
    return df


def update_db_table(dict_df, Table, table_name, session):
    """
    Actualiza la tabla en la base de datos.

    Args:
    - dict: diccionario con los elementos a actualizar.
    - Table: tabla de la base de datos.
    - table_name: nombre de la tabla a actualizar.
    - session: sesión de la base de datos.
    """
    try:
        for row in dict_df:
            rec = Table(**row)
            session.merge(rec)
        session.commit()
        dp.logger.info(f'Actualización tabla {table_name}')
    except Exception as e:
        dp.logger.error("Ocurrió un error:", e)
        session.rollback()
        pass


def capitalize_df(df):
    """
    Capitaliza todas las columnas de un DataFrame que sean de tipo string.

    Args:
    - df: DataFrame de Pandas a capitalizar.

    Returns:
    - DataFrame capitalizado.
    """
    return df.applymap(lambda x: x.capitalize() if isinstance(x, str) else x)


def find_file(path, name):
    """
    Busca un archivo que contenga el nombre proporcionado en el directorio indicado.

    Args:
        path: Objeto Path que representa el directorio donde se buscará el archivo.
        name: Cadena que debe aparecer en el nombre del archivo.

    Returns:
        El nombre completo del archivo que contiene el nombre proporcionado, o None si no se encuentra.
    """
    # Crear un patrón de búsqueda que contenga la cadena proporcionada en el nombre del archivo
    pattern = f'*{name}*'

    # Utilizar el método glob() para buscar archivos que coincidan con el patrón en el directorio proporcionado
    files = path.glob(pattern)

    # Iterar sobre los archivos encontrados y devolver el nombre del primer archivo
    # que contenga la cadena proporcionada
    for file in files:
        if name in file.name:
            return str(file)

    # Si no se encuentra ningún archivo, devolver None
    return None
