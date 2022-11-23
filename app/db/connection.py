import os
from sqlalchemy import create_engine
from sqlalchemy import MetaData

import dependencies as dp


def engine_create(db_type, db_name, user, password, host, port):
    if db_type == 'postgresql':
        engine = create_engine(
            f'postgresql://{user}:{password}@{host}:{port}/{db_name}'
        )
    elif db_type == 'mssql':
        engine = create_engine(
            f'mssql+pyodbc://{user}:{password}@{host}/{db_name}'
            f'?driver=ODBC+Driver+17+for+SQL+Server'
        )
    elif db_type == 'oracle':
        engine = create_engine(
            f'oracle://{user}:{password}@{host}:{port}/{db_name}'
        )
    else:
        raise Exception('Database type not supported')
    return engine


engine = engine_create(
    db_type='postgresql',
    db_name=dp.config[dp.enviroment]['database']['db'],
    user=os.environ.get('DATABASE_USER'),
    password=os.environ.get('DATABASE_PASSWORD'),
    host=dp.config[dp.enviroment]['database']['host'],
    port=5432
)
conn = engine.connect()
