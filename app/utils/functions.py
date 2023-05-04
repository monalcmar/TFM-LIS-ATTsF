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
    elif formato == "upper":
        columna = columna.str.upper()
    elif formato == "lower":
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
