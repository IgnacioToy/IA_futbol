from requests import request
import logging
import requests
from modelo_futbol.configuracion.proveedores import  PROVEEDORES
from modelo_futbol.configuracion.direcciones import DIR_RAW

logger = logging.getLogger(__name__)

class DescargarDatos:
    """
    Clase responsable del proceso completo de ingestión de datos.

    Responsabilidades:
        - Iterar todos los proveedores configurados.
        - Iterar ligas y temporadas.
        - Construir URLs dinámicamente.
        - Descargar archivos desde internet.
        - Guardarlos en la carpeta RAW.
        - Controlar errores sin detener el sistema.
        - Evita descargas duplicadas (idempotencia).
    """

    def __init__(self):
        # Inicia el descargador. Actualmente no requiere parámetros.
        pass

    # Punto de entrada principal:
    def run(self):
        """ Ejecuta el proceso completo de descarga para los proveedores. Retorna: Lista: Lista de errores ocurridos durante el proceso. """
        logger.info("Inicializando proceso de descarga de datos.")

        errores_totales = []

        for nombre_proveedores in PROVEEDORES.keys():                                       # Recorro los nombres de los proveedores en el diccionario de proveedores y busco su clave
            errores = self._descargar_proveedores(nombre_proveedores)                       # Inicio la descarga de los probedoores
            errores_totales.extend(errores)                                                 # Guardo los errores en la variable de errores totales

        if errores_totales:
            logger.warning(f"Proceso finalizado con {len(errores_totales)} errores")        # Si existen errores lanzo un 
        else:
            logger.info("Proceso finalizado sin errores.")

        return errores_totales                                                              # retorno la lista de los errores totales

    # Descarga por proveedores
    def _descargar_proveedores(self, nombre_proveedor: str):
        """ Descarga todos los datos asociados a un proveedor especifico.
        Args: nombre_proveedor(str): Nombre del proveedor definido en PROVEEDORES.
        Returns: List: lista de errores ocurriods en este proveedor. """
        if nombre_proveedor not in PROVEEDORES:
            raise ValueError(f"Proveedor '{nombre_proveedor}' no esta configurado.")

        configuracion = PROVEEDORES[nombre_proveedor]
        errores = []

        logger.info(f"Descargando datos del proveedor: {nombre_proveedor}")

        for nombre_liga, datos_liga in configuracion["ligas"].items():
            for temporada in datos_liga["temporadas"]:

                try:
                    url = self._construir_url(
                        configuracion = configuracion,
                        codigo_liga = datos_liga["codigo"],
                        temporada = temporada
                    )

                    logger.info(f"Descargando: {nombre_liga} - {temporada}")

                    response = requests.get(url, timeout=15)
                    response.raise_for_status()

                    archivo_guardado = self._guardar_archivo(
                        contenido = response.content,
                        nombre_proveedor = nombre_proveedor,
                        nombre_liga = nombre_liga,
                        temporada = temporada
                    )

                    if archivo_guardado:
                        logger.info(f"Guardado correctamente: {nombre_liga} {temporada}")
                    else:
                        logger.info(f"Archivo ya existente: {nombre_liga} {temporada}")

                except requests.RequestException as e:
                    logger.error(f"Error HTTP en {nombre_liga}, {temporada}: {e}")
                    errores.append((nombre_proveedor, nombre_liga, temporada, str(e)))
        
        return errores

    # Constructor de url:
    def _construir_url(self, configuracion: dict, codigo_liga: str, temporada: str):
        """
        Construye la URL final de descarga usando el patrón del proveedor.
        Args:
            configuracion (dict): Configuración del proveedor.
            codigo_liga(str): Código identificador de la liga.
            temporada (str): Temporada correspondiente.
        Returns:
            str: URL formateada lista para descargar.
        """
        return configuracion["url_pattern"].format(
            base = configuracion["base_url"],
            temporada = temporada,
            liga = codigo_liga
        )
    
    def _guardar_archivos(self, contenido: bytes, nombre_proveedor: str, nombre_liga: str, temporada: str) -> bool:
        """
        Guarda el archivo descargado en la carpeta RAW.
        Implementa: Creación automatica de carpetas, control de duplicados (bo sobreescribe), validación basica de contenido.
        Args:
            - Contendio (bytes): Contenido binario del archivo descargado.
            - nombre_proveedor (str): Nombre del proveedor.
            - temporada (str): Temporada.
        Reutns:
            - True -> Archivo guardado correctamente.
            - False -> Archivo ya existia y se omitio.
        """

        if not contenido:
            logger.warning("El archivo descargado esta vació. Se omite guardarlo.")
            return False
        
        subcarpeta = DIR_RAW / nombre_liga
        subcarpeta.mkdir(parents=True, exist_ok=True)

        nombre_archivo = f"{nombre_proveedor}_{nombre_liga}_{temporada}.csv"
        ruta_archivo = subcarpeta / nombre_archivo

        # Idempotencia: Evita la sobreescritura
        if ruta_archivo.exists():
            return False

        with open(ruta_archivo, "wb") as archivo:
            archivo.write(contenido)
        
        logger.info(f"Archivo guardado en: {ruta_archivo}")

        return True
