from typing import Any, Type, Generator
import pymongo
from pymongo import MongoClient, collection
from bson import ObjectId
from config import URL_DB, DB_NAME, CACHE_PORT, CACHE_HOST, CACHE_USERNAME, CACHE_PASSWORD
import datetime
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable
from geojson import Point
import logging
import time
import redis
import json
import threading

# Configuración del logger
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def get_location_point(address: str) -> Point:
    geolocator = Nominatim(user_agent="ODM/1.1 (nestorvillap@gmail.com)", timeout=10)
    for attempt in range(5):
        try:
            time.sleep(2)
            location = geolocator.geocode(address)
            if location:
                return Point((location.longitude, location.latitude))
        except (GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable):
            time.sleep(2 ** (attempt + 1))
    logger.warning(f"No se pudo obtener la ubicación para la dirección: {address}")
    return None

class Model:
    required_vars: set[str] = set()
    admissible_vars: set[str] = set()
    db: collection.Collection = None

    # Campos para modelos anidados y fechas
    _embedded_list_fields: list[str] = []
    _embedded_fields: list[str] = []
    _model_classes: dict[str, Type['Model']] = {}
    _date_fields: set[str] = set()
    _indexes: list = []

    # Referencias al cliente Redis de caché
    r_cache = None

    def __init__(self, **kwargs: Any):
        self._id = kwargs.pop('_id', None)
        if self._id and not isinstance(self._id, ObjectId):
            self._id = ObjectId(self._id)
        self._changed_fields = set()
        self._process_and_set_attributes(kwargs)

    def _process_and_set_attributes(self, attributes: dict):
        # Procesar campos anidados
        for field_name in self._embedded_list_fields + self._embedded_fields:
            if field_name in attributes:
                attributes[field_name] = self._process_embedded_field(field_name, attributes[field_name])

        # Procesar campos de fecha
        for field_name in self._date_fields:
            if field_name in attributes:
                attributes[field_name] = self._process_date_field(field_name, attributes[field_name])

        self.validate_attributes(attributes)
        self.__dict__.update(attributes)

    def validate_attributes(self, attributes: dict[str, Any]) -> None:
        missing_vars = self.required_vars - attributes.keys()
        if missing_vars:
            raise ValueError(f"Faltan variables requeridas: {missing_vars}")
        invalid_vars = attributes.keys() - (self.required_vars | self.admissible_vars)
        if invalid_vars:
            raise ValueError(f"Variables no admitidas: {invalid_vars}")

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith('_'):
            super().__setattr__(name, value)
        elif name in self.required_vars or name in self.admissible_vars:
            if getattr(self, name, None) != value:
                self._changed_fields.add(name)
            self.__dict__[name] = value
        else:
            # Para permitir atributos de clase como r_cache, etc.
            if hasattr(self, name) or name in ('r_cache', 'db'):
                super().__setattr__(name, value)
            else:
                raise AttributeError(f"No se puede asignar una variable no admitida: {name}")

    def to_dict(self) -> dict:
        doc = {}
        for k, v in self.__dict__.items():
            if k.startswith('_'):
                continue
            if isinstance(v, Model):
                doc[k] = v.to_dict()
            elif isinstance(v, list):
                doc[k] = [item.to_dict() if isinstance(item, Model) else item for item in v]
            else:
                doc[k] = v
        if self._id is not None:
            doc['_id'] = self._id
        return doc

    def to_update_dict(self) -> dict:
        update_doc = {}
        for field in self._changed_fields:
            value = getattr(self, field)
            if isinstance(value, Model):
                update_doc[field] = value.to_dict()
            elif isinstance(value, list):
                update_doc[field] = [item.to_dict() if isinstance(item, Model) else item for item in value]
            else:
                update_doc[field] = value
        return update_doc

    def pre_save(self):
        pass

    def save(self) -> None:
        self.pre_save()
        if self._id:
            if self._changed_fields:
                self.db.update_one({"_id": self._id}, {"$set": self.to_update_dict()})
                self._changed_fields.clear()
        else:
            self._id = self.db.insert_one(self.to_dict()).inserted_id
            self._changed_fields.clear()

        # Actualizar la caché con el objeto completo
        self._cache_set(self._id, self.to_dict())

    def delete(self) -> None:
        if self._id:
            self.db.delete_one({"_id": self._id})
            # Eliminar de la caché
            self._cache_delete(self._id)

    @classmethod
    def _cache_key(cls, key: str) -> str:
        return f"{cls.__name__}:{key}"

    @classmethod
    def _cache_set(cls, object_id: ObjectId, value: dict) -> None:
        if cls.r_cache:
            cls.r_cache.setex(cls._cache_key(str(object_id)), 86400, json.dumps(value, default=str))

    @classmethod
    def _cache_get(cls, object_id: ObjectId) -> dict:
        if cls.r_cache:
            data = cls.r_cache.get(cls._cache_key(str(object_id)))
            if data:
                # Renueva el TTL al acceder
                cls.r_cache.expire(cls._cache_key(str(object_id)), 86400)
                return json.loads(data)
        return None

    @classmethod
    def _cache_delete(cls, object_id: ObjectId) -> None:
        if cls.r_cache:
            cls.r_cache.delete(cls._cache_key(str(object_id)))

    @classmethod
    def _cache_query_key(cls, query_name: str) -> str:
        return f"{cls.__name__}:query:{query_name}"

    @classmethod
    def _cache_query_set(cls, key: str, results: list[dict]) -> None:
        if cls.r_cache:
            cls.r_cache.setex(cls._cache_query_key(key), 86400, json.dumps(results, default=str))

    @classmethod
    def _cache_query_get(cls, key: str) -> list[dict]:
        if cls.r_cache:
            data = cls.r_cache.get(cls._cache_query_key(key))
            if data:
                cls.r_cache.expire(cls._cache_query_key(key), 86400)
                return json.loads(data)
        return None

    @classmethod
    def _serialize_filter(cls, f: dict) -> str:
        return json.dumps(f, sort_keys=True, default=str)

    @classmethod
    def _serialize_pipeline(cls, p: list[dict]) -> str:
        return json.dumps(p, sort_keys=True, default=str)

    @classmethod
    def find(cls, filter: dict[str, Any]) -> 'ModelCursor':
        # Guardar la consulta en cache
        serialized_filter = cls._serialize_filter(filter)
        cached = cls._cache_query_get(serialized_filter)
        if cached is not None:
            # Devuelve directamente desde cache
            return ModelCursor(cls, cached, raw=False, from_cache=True)

        # Si no está en caché, consultar la BD y guardar en caché
        cursor = list(cls.db.find(filter))
        cls._cache_query_set(serialized_filter, cursor)
        return ModelCursor(cls, cursor, raw=False)

    @classmethod
    def find_by_id(cls, id: Any) -> 'Model':
        if isinstance(id, str):
            id = ObjectId(id)

        # Intentar obtener desde caché
        cached = cls._cache_get(id)
        if cached:
            return cls(**cached)

        # Si no está en cache, buscar en Mongo
        try:
            doc = cls.db.find_one({'_id': id})
            if doc:
                # Guardar en caché
                cls._cache_set(id, doc)
                return cls(**doc)
        except Exception as e:
            logger.warning(f"Error finding document by ID: {e}")

        return None

    @classmethod
    def aggregate(cls, pipeline: list[dict], raw: bool = False) -> 'ModelCursor':
        # Guardar la consulta en cache
        serialized_pipeline = cls._serialize_pipeline(pipeline)
        cached = cls._cache_query_get(serialized_pipeline)
        if cached is not None:
            return ModelCursor(cls, cached, raw=raw, from_cache=True)

        cursor = list(cls.db.aggregate(pipeline))
        cls._cache_query_set(serialized_pipeline, cursor)
        return ModelCursor(cls, cursor, raw=raw)

    @classmethod
    def init_class(cls, db_collection: collection.Collection, r_cache=None) -> None:
        cls.db = db_collection
        cls.r_cache = r_cache
        cls._create_indexes()

    @classmethod
    def _create_indexes(cls):
        for index in cls._indexes:
            cls.db.create_index(index)

    def _process_embedded_field(self, field_name: str, value):
        model_class = self._model_classes.get(field_name, Model)
        if isinstance(value, list):
            return [model_class(**item) if isinstance(item, dict) else item for item in value]
        elif isinstance(value, dict):
            return model_class(**value)
        elif isinstance(value, Model):
            return value
        else:
            raise ValueError(f"{field_name} debe ser un dict, una lista o una instancia de Model")

    def _process_date_field(self, field_name: str, value):
        if isinstance(value, str):
            for fmt in ('%Y-%m-%d', '%d/%m/%Y'):
                try:
                    return datetime.datetime.strptime(value, fmt)
                except ValueError:
                    continue
            try:
                return datetime.datetime.fromisoformat(value)
            except ValueError:
                raise ValueError(f"El campo {field_name} debe ser una fecha válida en formato 'YYYY-MM-DD' o 'DD/MM/YYYY'")
        elif isinstance(value, datetime.date):
            return datetime.datetime.combine(value, datetime.time())
        elif isinstance(value, datetime.datetime):
            return value
        else:
            raise ValueError(f"El campo {field_name} debe ser un objeto datetime o una cadena de fecha válida")

class Direccion(Model):
    required_vars = {"calle", "numero", "ciudad", "codigo_postal", "pais"}
    required_fields_order = ["calle", "numero", "portal", "piso", "codigo_postal", "ciudad", "pais"]
    admissible_vars = {"portal", "piso", "location"}
    _indexes = [("location", pymongo.GEOSPHERE)]

    def save(self):
        if not getattr(self, 'location', None):
            address_components = [str(getattr(self, key)) for key in self.required_fields_order if getattr(self, key, None)]
            address_str = ', '.join(address_components)
            self.location = get_location_point(address_str)
        super().save()

class Cliente(Model):
    required_vars = {"nombre", "fecha_alta"}
    admissible_vars = {"direcciones_facturacion", "direcciones_envio", "tarjetas_pago", "fecha_ultimo_acceso"}
    _embedded_list_fields = ['direcciones_facturacion', 'direcciones_envio']
    _model_classes = {'direcciones_facturacion': Direccion, 'direcciones_envio': Direccion}
    _date_fields = {'fecha_alta', 'fecha_ultimo_acceso'}

class Proveedor(Model):
    required_vars = {"nombre"}
    admissible_vars = {"direcciones_almacenes"}
    _embedded_list_fields = ['direcciones_almacenes']
    _model_classes = {'direcciones_almacenes': Direccion}

class Producto(Model):
    required_vars = {"nombre", "codigo_producto_proveedor", "precio", "dimensiones", "peso", "proveedores"}
    admissible_vars = {"coste_envio", "descuento_rango_fechas"}
    _embedded_list_fields = ['proveedores']
    _model_classes = {'proveedores': Proveedor}

class Compra(Model):
    required_vars = {"productos", "cliente", "precio_compra", "fecha_compra", "direccion_envio"}
    admissible_vars = set()
    _embedded_fields = ['direccion_envio', 'cliente', 'productos']
    _model_classes = {'direccion_envio': Direccion, 'cliente': Cliente, 'productos': Producto}
    _date_fields = {'fecha_compra'}
    _indexes = [("direccion_envio.location", pymongo.GEOSPHERE)]

class ModelCursor:
    def __init__(self, model_class: Type[Model], cursor, raw: bool = False, from_cache: bool = False):
        self.model_class = model_class
        self.raw = raw
        self.from_cache = from_cache
        # Si from_cache=True, cursor ya es una lista de dicts
        if from_cache:
            self.results = cursor
        else:
            self.results = list(cursor)

    def __iter__(self) -> Generator[Any, None, None]:
        for doc in self.results:
            if self.raw:
                yield doc
            else:
                yield self.model_class(**doc)


# ---------------------------------------------------------
# Empaquetado: Uso de Redis db=1 para manejar la cola y servicios

def empaquetar(compra_id: str, service_id: int, sleep_time: int = 2):
    # Este método simula el empaquetado
    print(f"Servicio {service_id} empaquetando compra {compra_id}...")
    time.sleep(sleep_time)

def packaging_service_main(r_queue, service_id=1):
    # Servicio principal: espera indefinidamente
    while True:
        # Espera indefinida: BLPOP sin timeout
        item = r_queue.blpop("pending_compras")
        if item:
            _, compra_id = item
            compra_id = compra_id.decode("utf-8")
            # Empaquetar compra
            empaquetar(compra_id, service_id)
            # Crear un nuevo servicio secundario
            new_service_id = service_id + 1
            t = threading.Thread(target=packaging_service_secondary, args=(r_queue, new_service_id))
            t.start()

def packaging_service_secondary(r_queue, service_id):
    # Servicio secundario: espera 1 minuto (60s) por una nueva compra
    item = r_queue.blpop("pending_compras", timeout=60)
    if item:
        _, compra_id = item
        compra_id = compra_id.decode("utf-8")
        empaquetar(compra_id, service_id)
        # Crear otro servicio secundario
        new_service_id = service_id + 1
        t = threading.Thread(target=packaging_service_secondary, args=(r_queue, new_service_id))
        t.start()
    else:
        # No se encontró ninguna compra en 1 minuto, termina el servicio secundario
        print(f"Servicio secundario {service_id} finaliza por inactividad.")


def enqueue_compra(r_queue, compra_id: str):
    # Encolar una compra confirmada para su empaquetado
    r_queue.rpush("pending_compras", compra_id)


# ---------------------------------------------------------

def init_app() -> None:
    # Mongo
    client = MongoClient(URL_DB)
    db = client[DB_NAME]

    # Redis caché (db=0)
    r_cache = redis.Redis(host=CACHE_HOST, port=CACHE_PORT,
                          username=CACHE_USERNAME,
                          password=CACHE_PASSWORD,
                          ssl=False, db=0)
    # Configuración de memoria
    r_cache.config_set('maxmemory', '150mb')
    # Política para eliminar claves con menor TTL primero
    r_cache.config_set('maxmemory-policy', 'volatile-ttl')

    # Inicializar clases con cache
    Cliente.init_class(db["cliente"], r_cache)
    Producto.init_class(db["producto"], r_cache)
    Compra.init_class(db["compra"], r_cache)
    Proveedor.init_class(db["proveedor"], r_cache)
    Direccion.init_class(db["direccion"], r_cache=None)  # Si es necesario

    # Redis cola (db=1) para empaquetado
    r_queue = redis.Redis(host=CACHE_HOST, port=CACHE_PORT,
                          username=CACHE_USERNAME,
                          password=CACHE_PASSWORD,
                          ssl=False, db=1)

    # Iniciar el servicio principal de empaquetado
    # Esto se podría iniciar en otro hilo o proceso.
    # Por simplicidad, se deja comentado aquí.
    # threading.Thread(target=packaging_service_main, args=(r_queue, 1), daemon=True).start()

    # Ejemplo de uso:
    # enqueue_compra(r_queue, "compra_12345")
