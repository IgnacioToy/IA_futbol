import logging
import pandas as pd
from pathlib import Path
from modelo_futbol.configuracion.direcciones import DIR_RAW, DIR_PROCESSED

logger = logging.getLogger(__name__)

class LimpiadorDatos():
    """ Clase responsable del procesamiento y limpieza de archivos RAW.
    Flujo: RAW -> Validación -> Normalización -> PROCESSED.
    diseñado inicialmente para football-data """

    def __init__(self):
        pass

    # ==========================================================
    # PUNTO DE ENTRADA
    # ==========================================================
    def ejecutar(self):
        # Ejecuta el proceso de limpieza para todos los archivos en RAW.
        logger.info("Iniciando el proceso de limpieza de datos.")

        archivos = list(DIR_RAW.rglob("*.csv"))                             # Busca todos los archivos en la carpeta RAW que terminen en ".csv"

        if not archivos:
            logger.warning("No se encontraron archivos en RAW.")
            return

        for archivo in archivos:
            try:
                logger.info(f"Procesando archivos: {archivo.name}")

                df = self._leer_csv(archivo)                            # Instancio la funcion de leer archivos del parametro archivo
                df = self._limpiar_datos(df)                                # Instancio la funcion para limpiar los datos

                self._guardar_procesado(df, archivo)

                logger.info(f"Archivo procesado correctamente.")
            
            except Exception as e:
                logger.error(f"Error procesando {archivo.name}: {e}")
        
        logger.info("Proceso de limpieza finalizado.")

    
    # ==========================================================
    # LECTURA
    # ==========================================================

    def _leer_csv(self, ruta: Path) -> pd.DataFrame:
        return pd.read_csv(ruta)


    # ==========================================================
    # LIMPIEZA
    # ==========================================================
    def _limpiar_datos(self, df: pd.DataFrame) -> pd.DataFrame:

        df = df.copy()                                                                  # Realizando una copia para poder trabajar mas comodo

        # Normalizamos nombres de las columnas
        df.columns = df.columns.str.strip()

        columnas_necesarias = ["Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"]   # Seleccionando las columnas necesarias para trabajar con el df

        for col in columnas_necesarias:                                                 # Recorrer las columnas para ver si estan todas ok
            if col not in df.columns:
                raise ValueError(f"Columna faltante: {col}")
            
        df["fecha"] = pd.to_datetime(df["Date"], dayfirst= True, errors= "coerce")      # 
        df = df.dropna(subset= ["fecha"])

        df = df.rename(columns = {
            "HomeTeam": "equipo_local",
            "AwayTeam": "equipo_visitante",
            "FTHG": "goles_local",
            "FTAG": "goles_visitante",
            "FTR": "resultado"
        })

        columnas_finales = ["fecha", "equipo_local", "equipo_visitante", "goles_local", "goles_visitante", "resultado"]

        return df[columnas_finales]

    
    # ==========================================================
    # GUARDADO EN PROCESSED
    # ==========================================================
    def _guardar_procesados(self, df: pd.DataFrame, ruta_original: Path):
        """ Giardar el DataFrame limpio en la carpeta PROCESSED. Mantiene el mismo nombre del archivo original """
        DIR_PROCESSED.mkdir(parents=True, exist_ok=True)                                # 

        nueva_ruta = DIR_PROCESSED / ruta_original.name

        df.to_csv(nueva_ruta, index=False)