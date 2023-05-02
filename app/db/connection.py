import os
from sqlalchemy import create_engine
from sqlalchemy import MetaData

import dependencies as dp


def engine_create(db_type, db_name, user, password, host, port, schema=None, setinputsizes=True):  #
    if db_type == 'postgresql':
        engine = create_engine(
            f'postgresql://{user}:{password}@{host}:{port}/{db_name}', connect_args=schema
        )
    elif db_type == 'mssql':
        engine = create_engine(
            f'mssql+pyodbc://{user}:{password}@{host}/{db_name}'
            f'?driver=SQL+Server', use_setinputsizes=setinputsizes  #
        )
    elif db_type == 'oracle':
        engine = create_engine(
            f'oracle://{user}:{password}@{host}:{port}/{db_name}'
        )
    else:
        raise Exception('Database type not supported')
    return engine


engine = engine_create(
    db_type='mssql',  #
    db_name=dp.config[dp.enviroment]['database']['db'],
    user=os.environ.get('DATABASE_USER'),
    password=os.environ.get('DATABASE_PASSWORD'),
    host=dp.config[dp.enviroment]['database']['host'],
    port=1433,
    setinputsizes=False
)
conn = engine.connect()