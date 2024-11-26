import configparser
import ast


def carga_propiedades():
    config = configparser.ConfigParser()
    config.read('E:/Users/1043077/Documents/Python/srv_cuentas/properties')
    propiedades = {}

    for seccion in config.sections():
        for key, value in config.items(seccion):
            try:
                evaluated_value = ast.literal_eval(value)
            except (ValueError, SyntaxError):
                evaluated_value = value
            propiedades[key] = evaluated_value
    return propiedades


def leer_propiedades(propiedad):
    propiedad = carga_propiedades().get(propiedad)
    return propiedad
