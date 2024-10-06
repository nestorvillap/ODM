from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Extraer las variables necesarias
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_USERNAME = os.getenv("DB_USERNAME")
URL_SERVER = os.getenv("URL_SERVER")
DB_NAME = os.getenv("DB_NAME")
URL_DB = f"mongodb+srv://{DB_USERNAME}:{DB_PASSWORD}{URL_SERVER}"