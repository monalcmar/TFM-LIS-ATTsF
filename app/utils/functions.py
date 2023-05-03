# ========== Modulos Python =============
"""Librerias públiclas de python."""
import re
import pandas as pd
# =========== Modulos propios ===========
import dependencies as dp

# Funcion que formatea columnas de texto


def columnas_texto(columna, formato):
    """
    Funcion que da formato a las columnas de texto (upper / title / capitalize).

    Quita espación en blanco al inicio y al final.
    """
    if formato == "capitalize":
        columna = columna.str.capitalize()
    if formato == "upper":
        columna = columna.str.upper()
    else:
        columna = columna.str.title()
    columna = columna.str.lstrip()
    columna = columna.str.rstrip()
    return (columna)


# Funcion que busca hoja d excel


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