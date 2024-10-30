from typing import Any, Type, Generator
import pymongo
from pymongo import MongoClient, collection
from bson import ObjectId
from config import URL_DB, DB_NAME
import datetime
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable
from geojson import Point
import logging
import time

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

    def delete(self) -> None:
        if self._id:
            self.db.delete_one({"_id": self._id})

    @classmethod
    def find(cls, filter: dict[str, Any]) -> 'ModelCursor':
        if "_id" in filter and isinstance(filter["_id"], str):
            filter["_id"] = ObjectId(filter["_id"])
        return ModelCursor(cls, cls.db.find(filter))

    @classmethod
    def aggregate(cls, pipeline: list[dict]) -> 'ModelCursor':
        cursor = cls.db.aggregate(pipeline)
        return ModelCursor(cls, cursor)

    @classmethod
    def init_class(cls, db_collection: collection.Collection) -> None:
        cls.db = db_collection
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

    def pre_save(self):
        if not getattr(self, 'location', None):
            address_components = [str(getattr(self, key)) for key in self.required_fields_order if getattr(self, key, None)]
            address_str = ', '.join(address_components)
            self.location = get_location_point(address_str)

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
    def __init__(self, model_class: Type[Model], cursor):
        self.model_class = model_class
        self.cursor = cursor

    def __iter__(self) -> Generator[Any, None, None]:
        for doc in self.cursor:
            yield self.model_class(**doc)

def init_app() -> None:
    client = MongoClient(URL_DB)
    db = client[DB_NAME]
    Cliente.init_class(db["cliente"])
    Producto.init_class(db["producto"])
    Compra.init_class(db["compra"])
    Proveedor.init_class(db["proveedor"])
