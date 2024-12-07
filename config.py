from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Extraer las variables necesarias
#Database
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_USERNAME = os.getenv("DB_USERNAME")
URL_SERVER = os.getenv("URL_SERVER")
DB_NAME = os.getenv("DB_NAME")
URL_DB = f"mongodb+srv://{DB_USERNAME}:{DB_PASSWORD}{URL_SERVER}"
#Cache
CACHE_HOST = os.getenv("CACHE_HOST")
CACHE_PORT = int(os.getenv("CACHE_PORT"))
CACHE_USERNAME = os.getenv("CACHE_USERNAME")
CACHE_PASSWORD = os.getenv("CACHE_PASSWORD")
#Neo4J
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")