"""
Archivo que contiene la configuración del logging
"""

import logging
from logging.handlers import RotatingFileHandler
from modelo_futbol.configuracion.direcciones import DIR_LOG

def config_log():
    logger = logging.getLogger()            
    logger.setLevel(logger.INFO)        # Sirve para que solo anote cosas que sean nivel INFO o superiores (como los errores)

    formateador = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )   # Que utilice el formato: Pone la fecha y hora exacta - Dice si es un aviso, error o solo informacion, El texto que escriba por defecto en los log

    # Consola:
    controlador_de_consola = logging.StreamHandler()        # Envia los mensajes a la terminal para que lo vea mientras el código corre
    controlador_de_consola.setFormatter(formateador)          # Que el controlador 

    # Archivo con rotación
    manejador_archivo = RotatingFileHandler(
        DIR_LOG / "ingesta.log",
        maxBytes= 5_000_000,                                # 5MB
        backupCount=3
    )
    manejador_archivo.setFormatter(formateador)

    logger.addHandler(controlador_de_consola)
    logger.addHandler(manejador_archivo)