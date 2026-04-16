import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# Intentar importar configuración centralizada para separar credenciales del código lógico
try:
    import config
except ImportError:
    raise ImportError("Falta archivo 'config.py' con API_BASE_URL, TOKEN y ENDPOINTS.")

# Configuración de logging para trazabilidad en entornos de producción
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DataPipeline")

class APIClient:
    """
    Clase de interfaz para la API. Encapsula la lógica de red para asegurar 
    que el resto del pipeline sea independiente del protocolo HTTP.
    """
    
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """
        Implementa una sesión persistente con estrategia de reintentos para 
        manejar la inestabilidad de red y errores temporales del servidor.
        """
        session = requests.Session()
        
        # El backoff_factor aplica una espera exponencial para evitar saturar el servidor en fallos
        # Se define un total de 5 reintentos para cubrir micro-cortes prolongados
        retry_strategy = Retry(
            total=5,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        # Establecemos headers por defecto para asegurar la consistencia del formato de datos
        session.headers.update({"Accept": "application/json"})
        return session

    def fetch(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Ejecuta peticiones GET. Implementa timeouts diferenciados para evitar el bloqueo
        indefinido de recursos: 3s para conectar y 25s para esperar la descarga de datos.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        query_params = params or {}
        query_params['token'] = self.token
        
        try:
            logger.info(f"Iniciando descarga de datos desde: {url}")
            response = self.session.get(url, params=query_params, timeout=(3, 25))
            # Eleva una excepción si el status code es 4xx o 5xx
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            # Manejo específico para Rate Limiting (exceso de peticiones)
            if e.response.status_code == 429:
                logger.error("Rate limit excedido. Se requiere revisar cuotas de API.")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Error de conexión en el endpoint {url}: {e}")
            raise

class DataProcessor:
    """
    Contiene la lógica de transformación. Diseñada para procesar datos en memoria 
    garantizando la integridad de los tipos de datos (Data Cleaning).
    """

    @staticmethod
    def clean_orders(raw_json: Dict[str, Any]) -> pd.DataFrame:
        """
        Parsea el JSON crudo, normaliza tipos y genera columnas derivadas 
        para análisis o particionamiento.
        """
        # Navegación segura en el diccionario para prevenir errores de clave inexistente
        orders = raw_json.get('tables', {}).get('orders', [])
        
        if not orders:
            logger.warning("Payload recibido sin registros de órdenes.")
            return pd.DataFrame()

        df = pd.DataFrame(orders)

        # Casteo de tipos: 'coerce' transforma errores en NaT/NaN para limpieza posterior
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')

        # Eliminamos registros incompletos que afectarían la calidad de los reportes (Data Quality)
        df = df.dropna(subset=['order_date', 'total_amount']).copy()

        # Feature Engineering: Extraemos año y mes para optimizar el almacenamiento físico
        df['year'] = df['order_date'].dt.year
        df['month'] = df['order_date'].dt.month
        
        # Regla de negocio: Clasificación de valor de transacciones
        df['is_high_value'] = df['total_amount'] > 100
        
        logger.info(f"Transformación completada: {len(df)} registros válidos.")
        return df

class DataStorage:
    """
    Abstrae la capa de persistencia. Se encarga de la escritura eficiente 
    en formatos optimizados para analítica (OLAP).
    """

    @staticmethod
    def save_parquet(df: pd.DataFrame, path: str):
        """
        Guarda los datos en formato Parquet. El particionamiento por año/mes 
        permite 'Data Skipping' en futuras consultas, mejorando el rendimiento.
        """
        if df.empty:
            logger.warning("Intento de guardado de un DataFrame vacío. Operación omitida.")
            return

        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            # engine='pyarrow' es el estándar para manejo de tipos complejos y alto desempeño
            df.to_parquet(
                path,
                engine='pyarrow',
                index=False,
                partition_cols=['year', 'month']
            )
            logger.info(f"Escritura completada. Destino: {path} | Registros: {len(df)}")
        except Exception as e:
            logger.error(f"Falla crítica al escribir en disco: {e}")
            raise

def run_etl():
    """
    Orquestador del flujo ETL. Controla la ejecución secuencial y el ciclo de vida 
    de los objetos de extracción, transformación y carga.
    """
    start_time = datetime.now()
    logger.info("--- Iniciando ciclo de ejecución del Pipeline ---")
    
    # Inyección de dependencias desde configuración externa
    client = APIClient(config.API_BASE_URL, config.API_TOKEN)
    
    try:
        # Fase de Extracción: 5000 registros para balancear carga y memoria
        raw_data = client.fetch("datasets.php", params={'type': 'ecommerce', 'rows': 5000})
        
        # Fase de Transformación
        processor = DataProcessor()
        df = processor.clean_orders(raw_data)
        
        # Fase de Carga: Capa 'Gold' representa datos listos para consumo final
        storage = DataStorage()
        storage.save_parquet(df, "output/orders_gold")
        
        # Cálculo de métrica de performance (SLA de ejecución)
        duration = datetime.now() - start_time
        logger.info(f"--- Pipeline finalizado exitosamente en {duration} ---")
        
    except Exception as e:
        # Gestión de excepciones remanentes para asegurar la trazabilidad en los logs
        logger.critical(f"El proceso falló debido a un error no controlado: {e}")

if __name__ == "__main__":
    run_etl()
