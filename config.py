import os
from dotenv import load_dotenv

# Carga las variables del archivo .env
load_dotenv()

# Asignación de variables
API_TOKEN = os.getenv('API_TOKEN')
API_BASE_URL = os.getenv('API_BASE_URL', 'https://iansaura.com/api')
USER_EMAIL = os.getenv('MY_EMAIL')

# Validación de seguridad básica
if not API_TOKEN:
    raise ValueError("Error: API_TOKEN no configurado en el archivo .env")