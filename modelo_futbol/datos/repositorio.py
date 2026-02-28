"""
CONSOLIDOR DE DATOS
===================

Este módulo se encarga de unificar todos los archivos CSV de la carpeta PROCESSED. Su función es crear un "DATASET MAESTRO" único, 
ordenado cronológicamente, que servirá como fuente oficial de datos para el entrenamiento del modelo.

Flujo:
1. Lee todos los CSV de data/PROCESSED.
2. Identifica la liga y temporada por el nombre del archivo.
3. Une todas las tablas en una sola.
4. Normaliza las fechas y ordena los partidos del mas viejo al más nuevo.
5. Guarda el resultado en data/final/dataset_maestro.csv
"""

import logging
import pandas as pd
from pathlib import Path
from modelo_futbol.configuracion.direcciones import DIR_PROCESSED, DIR_FINAL

logger = logging.getLogger(__name__)

class RepositorioDatos:
    """CONSOLIDOR DE DATASET (Clase Maestra)

    Esta clase actúa como el "unificador" del sistema. Su trabajo es escanear la carpeta de datos ya procesados, leer cada liga y temporada
    por separado y fusionarlas en un único archivo maestro (.csv) ordenado por tiempo

    Atributos:
        - DIR_PROCESSED (Path): Origen de los archivos individuales.
        - DIR_FINAL (Path): Destino donde se guardará el dataset unificado.

    Procesi:
    1. Recolecta todos los archivos CSV de la carpeta 'processed'.
    2. Inyecta columnas de 'loga' y 'temporada' basadas en el nombre del archivo.
    3. Concatena todos los datos en un solo DataFrame.
    4. Asegura que las fechas sean cronológicas para que la IA aprenda en orden correcto.
    5. Exporta el 'dataset_maestro.csv'.
    """

    def __init__(self):
        pass

    def ejecutar(self): 
        """ Motor principal: Coordina la lectura, unión y ordenamiento de datos."""
        logger.info("Iniciando consolidación del DataSet.")

        # Buscar archivos: list() convierte el buscador en una lista de rutas reales
        archivos = list(DIR_PROCESSED.glob("*.csv"))

        if not archivos:
            logger.warning("No se encontraron archivos procesados.")
            return
        dataframes = []

        # Bucle de lectura: archivo por archivo
        for archivo in archivos:
            try:
                logger.info(f"Consolidando {archivo.name}")
                df = pd._read_csv(archivo)

                # Extraccion info del nombre de la liga
                liga, temporada = slef._extraer_metadatos(archivo)


                # Se agregan estas etiquetas como columnas nuevas para no perder el origen.
                df["liga"] = liga
                df["temporada"] = temporada

                dataframes.append(df)                   # Agrego el dataframe uno abajo de otro.delatt
            
            except Exception as e:
                logger.error(f"Error al procesar {archivo.name}: {e}")

        if not dataframes:
            logger.warning("La lista de tablas esta vacias. Nda que consilar.")
            return
    
        # Unificación: Pegar todos los archivos uno debajo del otro
        df_final = pd.concat(dataframes, ignore_index=True)

        # Limpieza cronologica:
        df_final["fechas"]