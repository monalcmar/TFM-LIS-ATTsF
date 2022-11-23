import os
import subprocess
import requests
import json
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel
from fastapi import APIRouter
import pandas as pd
from dotenv import load_dotenv

# =========== Modulos propios ===========
from app.utils.connections import database_connection_old
from src.etl.etl import etl, etl_replanificar
from utils.travel_time import travel_info
from src.vector_extractor.extractor import extractor
from logger.logger import logger
import dependencies


router = APIRouter()
logger = logger()


@router.get("/test")
def test():
    logger.info("TEST")
    return('Ok')


@router.get('/vector')
def vector():
    extractor()


@router.get("/check_execution")
def check_execution():
    execution_control_file = f'running_planner'
    if os.path.exists(dependencies.rootFolder / 'temp' / execution_control_file):
        status = 1  # En ejecucion
    else:
        status = 0  # Planificador parado

    return({"Status": status})


@router.get("/planificador")
def run_planner():
    try:
        # execution_control_file = f'running_planner_{datetime.today().strftime("%Y-%m-%d")}'
        execution_control_file = f'running_planner'
        if not os.path.exists(dependencies.rootFolder / 'temp' / execution_control_file):
            open(dependencies.rootFolder / 'temp' / execution_control_file, 'w').close()

            logger.info("Iniciando ETL")
            etl()
            logger.info("Planificando")
            subprocess.call(
                [
                    'java',
                    '-jar',
                    f'{dependencies.rootFolder / "planner" / "Ejecutables" / "ganuza.jar"}',
                    'DEV',
                    'Planificacion',
                    '500000'
                ]
            )
            logger.info("Proceso terminado")

            os.remove(dependencies.rootFolder / 'temp' / execution_control_file)

        else:
            logger.("Planificador en ejerrorecución")

    except Exception as e:
        logger.exception(e, exc_info=True)
        if os.path.exists(dependencies.rootFolder / 'temp' / execution_control_file):
            os.remove(dependencies.rootFolder / 'temp' / execution_control_file)

    return {"Status": 1}


@router.get("/replanificar")
def replanificar():
    try:
        # execution_control_file = f'running_planner_{datetime.today().strftime("%Y-%m-%d")}'
        execution_control_file = f'running_planner'
        if not os.path.exists(dependencies.rootFolder / 'temp' / execution_control_file):
            open(dependencies.rootFolder / 'temp' / execution_control_file, 'w').close()

            logger.info("Replanificacion - Iniciando ETL")
            etl_replanificar()
            logger.info("Replanificacion - Planificando")
            subprocess.call(
                [
                    'java',
                    '-jar',
                    f'{dependencies.rootFolder.parent / "planner" / "Ejecutables" / "ganuza.jar"}',
                    'DEV',
                    'Replanificacion',
                    '500000'
                ]
            )
            logger.info("Replanificacion - Proceso terminado")

            os.remove(dependencies.rootFolder / 'temp' / execution_control_file)

        else:
            logger.error("Replanificacion - Planificador en ejecución")

    except Exception as e:
        logger.exception(e, exc_info=True)
        if os.path.exists(dependencies.rootFolder / 'temp' / execution_control_file):
            os.remove(dependencies.rootFolder / 'temp' / execution_control_file)

    return {"Status": 1}


class IdViaje(BaseModel):
    id_viaje: int


@router.post("/travel_time")
def travel_time(id_viaje: IdViaje):
    print(id_viaje)

    id_viaje = id_viaje.id_viaje

    with open(Path(__file__).parent / 'config' / 'config.json') as config_file:
        config = json.load(config_file)

    load_dotenv(dependencies.rootFolder / '.env')

    db_conn_ganuza = database_connection_old.Database(
        db_type='postgresql',
        db_name=dependencies.config['database']['db'],
        user=os.environ.get('DATABASE_USER'),
        password=os.environ.get('DATABASE_PASSWORD'),
        host=dependencies.config['database']['host'],
        port=5432
    )

    df_viaje = db_conn_ganuza.read(
        f'''
        SELECT
            id, "idObra", "idFase", "idTipoViaje", "fechaInicioPrev", "fechaFinPrev"
        FROM viaje
        WHERE id = {id_viaje}
        ''',
        type='dataframe'
    )

    df_viaje['fechaInicioPrev'] = pd.to_datetime(df_viaje['fechaInicioPrev'])
    df_viaje['fechaFinPrev'] = pd.to_datetime(df_viaje['fechaFinPrev'])

    df_obra = db_conn_ganuza.read(
        f'''
        SELECT
            id, "idEmpresa"
        FROM obra
        WHERE id = {df_viaje.loc[0, 'idObra']}
        ''',
        type='dataframe'
    )

    df_fase = db_conn_ganuza.read(
        f'''
        SELECT
            id, posicion
        FROM fase
        WHERE id = {df_viaje.loc[0, 'idFase']}
        ''',
        type='dataframe'
    )

    df_almacen = db_conn_ganuza.read(
        '''
        SELECT
            id, posicion
        FROM almacen
        ''',
        type='dataframe'
    )

    df = df_almacen.append(df_fase)

    df_tiempo = travel_info(
        df=df,
        token_openrouteservice=os.environ.get('TOKEN_OPENROUTESERVICE'),
        column_id='id',
        column_coordinates='posicion'
    )
    #  COMETNAR BIEN ESTA PARTE -> ES CUTRE
    if df_viaje.loc[0, 'idTipoViaje'] == 1:
        df_tiempo = df_tiempo.query('origen < 10 ' and 'destino > 10')

    elif df_viaje.loc[0, 'idTipoViaje'] == 2:
        df_tiempo = df_tiempo.query('origen > 10' and 'destino < 10')

    df_tiempo = df_tiempo.loc[df_tiempo['tiempo'] == df_tiempo['tiempo'].min()]
    df_tiempo['tiempo'] = pd.to_timedelta(df_tiempo['tiempo'], unit='minutes')

    # !!! METER EL TIEMPO DE OPERACION -> No. Cargan el camion el dia antes

    with open('/app/log/travel_time.txt', 'a') as f:
        f.write(f"{datetime.today().strftime('%Y-%m-%d_%H:%M:%S')}, {id_viaje}, {df_viaje['fechaInicioPrev'][0]}, {df_viaje['fechaFinPrev'][0]}, {df_tiempo['tiempo'][0]}\n")

    if (df_viaje['fechaInicioPrev'][0].date() == df_viaje['fechaFinPrev'][0].date()):
        df_viaje['horaSalidaMax'] = df_viaje['fechaFinPrev'] - df_tiempo['tiempo']
        df_viaje['horaSalidaMax'] = pd.to_datetime(df_viaje['horaSalidaMax'])
        df_viaje.set_index('horaSalidaMax', drop=False, inplace=True)

        if not df_viaje.between_time('00:00', '7:45').empty:
            hora_salida = df_viaje.reset_index(drop=True).loc[0, 'horaSalidaMax']
            hora_salida = hora_salida.strftime("%H:%M")

            URL = f"http://{dependencies.config['server']['ip']}/falloPlanificacionManual"

            respuesta = {
                "id_viaje": id_viaje,
                "hora_salida": hora_salida
            }
            print(respuesta)

            r = requests.post(url=URL, json=respuesta)

            if r.status_code == 200:
                f = open(dependencies.rootFolder / 'logger' / 'log' / 'connection.txt', 'a')
                f.write(f"{datetime.now()}, Response: {r.status_code}, ID Viaje: {id_viaje}.\n")
                f.close()

            else:
                f = open(dependencies.rootFolder / 'logger' / 'log' / 'connection.txt', 'a')
                f.write(f"{datetime.now()}, Response: {r.status_code}")
                f.close()

            return {"id_viaje": id_viaje, "hora_salida": hora_salida}

    return {"status": "Sin Interferencias"}
