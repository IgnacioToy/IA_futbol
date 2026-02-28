"""
Carpeta que va acontener los distintos proveedores para realizar la extraccion de los datos
"""

PROVEEDORES = {
    "football_data": {

        "base_url": "https://www.football-data.co.uk",

        "url_pattern": "{base}/mmz4281/{temporada}/{liga}.csv",

        "ligas": {
            "premier_league": {  # Coincide con tu carpeta
                "codigo": "E0",
                "temporadas": ["2122", "2223", "2324", "2425", "2526"]
            },
            "la_liga": {        # Coincide con tu carpeta
                "codigo": "SP1",
                "temporadas": ["2122", "2223", "2324", "2425", "2526"]
            },
            "serie_a": {        # Coincide con tu carpeta
                "codigo": "I1",
                "temporadas": ["2122", "2223", "2324", "2425", "2526"]
            },
            "bundesliga": {     # Por si querés agregarla
                "codigo": "D1",
                "temporadas": ["2122", "2223", "2324", "2425", "2526"]
            },
            "ligue_1": {        # Francia
                "codigo": "F1",
                "temporadas": ["2122", "2223", "2324", "2425", "2526"]
            }
        }
    }
}