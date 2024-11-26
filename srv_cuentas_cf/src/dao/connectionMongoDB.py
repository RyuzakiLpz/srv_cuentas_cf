from loadProperties import leer_propiedades
from pymongo import MongoClient
from src.resources.configlog import log_trace
from src.resources.common.level import Nivel


def conexion_mongodb(collection_name):
    try:
        cliente = MongoClient(leer_propiedades("cliente"), leer_propiedades("port"))
        db = cliente[leer_propiedades("db.name")]
        collections = db[collection_name]
        log_trace(Nivel.INFORMATIVO, f'Conexión Mongo a {collection_name} establecida con éxito')
        return collections
    except Exception as e:
        log_trace(Nivel.ERROR, str(e))
        return
