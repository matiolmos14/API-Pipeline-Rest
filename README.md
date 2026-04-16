# Pipeline ETL con API-REST, Pandas y Parquet
Este proyecto implementa un ecosistema de extracción automatizada (ETL) diseñado para interactuar con APIs REST de alta demanda. El pipeline transforma datos transaccionales crudos en un repositorio **OLAP optimizado (Capa Gold)**, garantizando la integridad del dato y la eficiencia en el consumo analítico posterior.

## Objetivo del Proyecto
Desarrollar un sistema de ingesta robusto que actúe como puente entre fuentes externas y un Data Lake local, priorizando el **resiliencia de red** y la **estandarización del dato** para facilitar la toma de decisiones basada en evidencia.


## Comprensión del Entorno (Visión Técnica y de Negocio)

Para asegurar que este proyecto aporte valor tanto a niveles de Management como a Equipos Técnicos, se detallan los pilares del diseño:

### ¿Qué significa una "Ingesta Resiliente"?
En entornos productivos, las APIs pueden presentar inestabilidad o límites de tráfico (Rate Limits).

### Visión Técnica:
El sistema utiliza una sesión persistente con **Exponential Backoff**. Si el servidor falla o limita la conexión, el pipeline "respira" y reintenta la operación de forma inteligente.

### Visión de Negocio: 
Se garantiza la continuidad operativa. Un fallo en la ingesta significa un día sin reportes de ventas; este sistema mitiga ese riesgo financiero y asegura la disponibilidad de la información.

### Almacenamiento Orientado a Consultas (Capa Gold)
* **Eficiencia de Costos:** 
Se seleccionó el formato Parquet por su alta capacidad de compresión y almacenamiento columnar, reduciendo costos de almacenamiento en la nube.

* **Optimización Analítica:** 
El uso de Hive Partitioning permite que herramientas de BI (Power BI, Athena, DuckDB) realicen Data Skipping, leyendo solo los archivos necesarios y reduciendo drásticamente los tiempos de respuesta para el usuario final.

## Stack Tecnológico
* **Lenguaje:** Python 3.10+
* **Protocolo:** REST API (JSON)
* **Procesamiento de Datos:** Pandas & Numpy.
* **Motor de Persistencia:** PyArrow (Escritura columnar de alto rendimiento).
* **Gestión de Red:** Requests con estrategias de reintento avanzadas (urllib3).

## Arquitectura del Pipeline
El script implementa una arquitectura modular basada en Programación Orientada a Objetos (POO) para asegurar el desacoplamiento y la mantenibilidad:

1. **APIClient:** Abstracción de la capa de red. Maneja la persistencia de conexión, cabeceras de autenticación y la lógica de reintentos.

2. **DataProcessor:** Lógica de saneamiento. Realiza el casteo de tipos, limpieza de nulos y generación de dimensiones temporales (Feature Engineering).

3. **DataStorage:** Capa de persistencia encargada de la jerarquía de archivos en disco y el particionamiento físico.

## Recomendaciones Estratégicas e Impacto Técnico
1. **Resiliencia ante Fallos de Infraestructura**
* **Evidencia:** Las APIs suelen presentar errores temporales 5xx o limitaciones 429 (Too Many Requests).

* **Acción:** Se implementó un factor de espera exponencial. Esto reduce la fatiga de infraestructura y asegura que el proceso finalice correctamente incluso ante inestabilidad del proveedor externo.

2. **Saneamiento y Calidad del Dato (SLA)**
* **Evidencia:** Se detectaron inconsistencias en los tipos de datos de montos y fechas en la fuente original que podrían corromper análisis posteriores.

* **Acción:** El pipeline aplica un filtrado defensivo y casteo numérico riguroso. Esto garantiza que el equipo de Business Intelligence trabaje sobre datos curados, eliminando errores de cálculo en reportes financieros.

3. Escalabilidad y Data Skipping
* **Evidencia:** Consultar archivos únicos de gran tamaño degrada el rendimiento de los motores analíticos.

* **Acción:** El almacenamiento particionado por year y month asegura que el pipeline sea escalable. Esto permite procesar millones de registros optimizando el uso de CPU y memoria al filtrar consultas por periodos específicos.

## Estructura del Proyecto

```text
├── config.py            # Gestión de credenciales y variables (API_TOKEN)
├── main.py              # Script principal (Orquestador del ETL)
├── requirements.txt     # Dependencias del sistema
├── output/              # Data Lake Local (Capas de datos)
│   └── orders_gold/     # Datos procesados y particionados (Parquet)
└── README.md            # Documentación estratégica
```

## Configuración e Instalación
Este proyecto utiliza un entorno virtual para asegurar que las dependencias sean locales y reproducibles.

1. **Clonar el repositorio**
```Bash
git clone https://github.com/matiolmos14/pipeline-api-rest
cd pipeline-api-rest
```

2. **Preparación del Entorno (Opcional)**
```Bash
python -m venv venv

# Activar en Windows:
.\venv\Scripts\activate
# Activar en Mac/Linux:
source venv/bin/activate
```

3. **Instalación de Dependencias e Infraestructura**
```Bash
pip install -r requirements.txt
```

4. **Configuración de Variables de Entorno**
Crea un archivo llamado .env en la raíz del proyecto y agrega el siguiente contenido con tus credenciales:
```Bash
API_BASE_URL=https://iansaura.com/api/datasets.php
MY_EMAIL=email@ejemplo.com
API_TOKEN=token
API_TYPE=ecommerce
API_ROWS=5000
```

## Ejecución
Para correr el pipeline completo, ejecuta el script principal:

```Bash
python main.py
```

Deberías ver los logs en la consola indicando el progreso:
```Bash
2026-04-15 21:06:27,789 - DataPipeline - INFO - Iniciando descarga de datos desde: https://iansaura.com/api/datasets.php
2026-04-15 21:06:29,277 - DataPipeline - INFO - Transformación completada: 5000 registros válidos.
2026-04-15 21:06:29,605 - DataPipeline - INFO - Escritura completada. Destino: output/orders_gold | Registros: 5000
2026-04-15 21:06:29,605 - DataPipeline - INFO - --- Pipeline finalizado exitosamente en 0:00:01.822539 ---
```

## Estructura de Salida
Los datos se guardarán automáticamente en la carpeta output/, organizados jerárquicamente por fecha:

```text
output/
└── orders/                 
    ├── year=2023/          
    │   ├── month=1/        
    │   │   └── data.parquet
    │   └── month=2/
    └── year=2024/
        └── month=10/
            └── data.parquet
```
