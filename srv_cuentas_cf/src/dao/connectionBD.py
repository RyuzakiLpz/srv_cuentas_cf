from sqlalchemy import create_engine
from loadProperties import leer_propiedades
from src.resources.configlog import log_trace, configure_logging
from src.resources.common.level import Nivel
from sqlalchemy.orm import sessionmaker

configure_logging()


def conexion_sql():
    try:
        engine = create_engine(
            f'mssql+pyodbc://{leer_propiedades("server")}/{leer_propiedades("database")}?trusted_connection=yes'
            f'&driver=ODBC+Driver+17+for+SQL+Server',
            connect_args={'charset': 'utf8'})
        log_trace(Nivel.INFORMATIVO, 'Conexión sql establecida con éxito')
        return engine
    except Exception as e:
        log_trace(Nivel.ERROR, str(e))
        return


def execute_session():
    engine = conexion_sql()
    if not engine:
        return None

    Session = sessionmaker(bind=engine)

    try:
        with Session() as session:
            return session
    except Exception as e:
        log_trace(Nivel.ERROR, f'Error durante la ejecución: {e}')
        return None


def execute_procedure_batch(proc_name, params_list=None):
    conn = conexion_sql().raw_connection()
    try:
        with conn.cursor() as cursor:
            if params_list is None:
                cursor.execute(f"EXEC {proc_name}")
                return cursor.fetchall()
            else:
                for params in params_list:
                    param_placeholders = ', '.join('?' * len(params))
                    query = f"EXEC {proc_name} {param_placeholders}"
                    cursor.execute(query, params)
                conn.commit()
            log_trace(Nivel.INFORMATIVO, f'Procedimiento {proc_name} ejecutado con éxito para el lote.')
    except Exception as e:
        log_trace(Nivel.ERROR, f"Error durante la ejecución de execute_procedure_batch: {e}")
        conn.rollback()
        return
    finally:
        conn.close()

