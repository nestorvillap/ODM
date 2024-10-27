__author__ = 'Pablo Ramos Criado'
__students__ = 'Nestor Villa Perez y Nicolas Fernandez Perez'

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
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_location_point(address: str) -> Point:
    geolocator = Nominatim(user_agent="ODM/1.1 (nestorvillap@gmail.com)", timeout=10)
    for attempt in range(5):
        try:
            logger.info(f"Geocodificando dirección: {address}")
            time.sleep(2)
            location = geolocator.geocode(address)
            if location:
                logger.info(f"Coordenadas encontradas: ({location.latitude}, {location.longitude})")
                return Point((location.longitude, location.latitude))
        except (GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable) as e:
            logger.warning(f"Error al geocodificar: {e}. Reintentando ({attempt + 1}/5)...")
            time.sleep(2 ** (attempt + 1))
    logger.warning(f"No se pudo obtener la ubicación para la dirección: {address}")
    return None

class Model:
    required_vars: set[str] = set()
    admissible_vars: set[str] = set()
    db: collection.Collection = None

    # Campos para modelos anidados y referencias
    _embedded_list_fields: list[str] = []
    _embedded_fields: list[str] = []
    _reference_list_fields: list[str] = []
    _reference_fields: list[str] = []
    _model_classes: dict[str, Type['Model']] = {}
    _denormalized_fields: list[str] = []
    _denormalized_list_fields: list[str] = []
    _date_fields: set[str] = set()

    def __init__(self, **kwargs: Any):
        self._id = kwargs.pop('_id', None)
        if self._id and not isinstance(self._id, ObjectId):
            self._id = ObjectId(self._id)
        self._changed_fields = set()
        self._process_and_set_attributes(kwargs)

    def _process_and_set_attributes(self, attributes: dict):
        # Procesar campos anidados y referencias
        for field_name in self._embedded_list_fields:
            if field_name in attributes:
                attributes[field_name] = self._process_embedded_list_field(field_name, attributes[field_name])
        for field_name in self._embedded_fields:
            if field_name in attributes:
                attributes[field_name] = self._process_embedded_field(field_name, attributes[field_name])
        for field_name in self._reference_list_fields:
            if field_name in attributes:
                attributes[field_name] = self._process_reference_list_field(attributes[field_name])
        for field_name in self._reference_fields:
            if field_name in attributes:
                attributes[field_name] = self._process_reference(attributes[field_name])
        # Procesar campos denormalizados
        for field_name in self._denormalized_fields:
            if field_name in attributes:
                model_class = self._model_classes.get(field_name, Model)
                value = attributes[field_name]
                attributes[field_name] = model_class(**value) if isinstance(value, dict) else value
        for field_name in self._denormalized_list_fields:
            if field_name in attributes:
                model_class = self._model_classes.get(field_name, Model)
                value = attributes[field_name]
                if isinstance(value, list):
                    attributes[field_name] = [model_class(**item) if isinstance(item, dict) else item for item in value]
        # Procesar campos de fecha
        for field_name in self._date_fields:
            if field_name in attributes:
                value = attributes[field_name]
                if isinstance(value, str):
                    for fmt in ('%Y-%m-%d', '%d/%m/%Y'):
                        try:
                            attributes[field_name] = datetime.datetime.strptime(value, fmt)
                            break
                        except ValueError:
                            continue
                    else:
                        try:
                            attributes[field_name] = datetime.datetime.fromisoformat(value)
                        except ValueError:
                            raise ValueError(f"El campo {field_name} debe ser una fecha válida en formato 'YYYY-MM-DD' o 'DD/MM/YYYY'")
                elif isinstance(value, datetime.date):
                    attributes[field_name] = datetime.datetime.combine(value, datetime.time())
                elif not isinstance(value, datetime.datetime):
                    raise ValueError(f"El campo {field_name} debe ser un objeto datetime o una cadena de fecha válida")
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

    def to_dict(self, processed_objects=None) -> dict:
        if processed_objects is None:
            processed_objects = set()
        if id(self) in processed_objects:
            return self._id if hasattr(self, '_id') else None
        processed_objects.add(id(self))

        def convert_value(val):
            if isinstance(val, list):
                return [convert_value(item) for item in val]
            elif isinstance(val, dict):
                if "type" in val and val["type"] == "Point" and "coordinates" in val:
                    return {k: v if k != "coordinates" else [float(c) for c in v] for k, v in val.items()}
                return {k: convert_value(v) for k, v in val.items()}
            elif isinstance(val, datetime.datetime):
                return val
            elif hasattr(val, 'to_dict'):
                return val.to_dict(processed_objects)
            else:
                return val

        doc = {}
        for k, v in self.__dict__.items():
            if k.startswith('_'):
                continue
            doc[k] = convert_value(v)
        if self._id is not None:
            doc['_id'] = self._id
        return doc

    def to_update_dict(self) -> dict:
        update_doc = {}
        for field in self._changed_fields:
            value = getattr(self, field)
            if hasattr(value, 'to_dict'):
                update_doc[field] = value.to_dict()
            elif isinstance(value, list):
                update_doc[field] = [item.to_dict() if hasattr(item, 'to_dict') else item for item in value]
            else:
                update_doc[field] = value
        return update_doc

    def pre_save(self):
        pass

    def save(self) -> None:
        self._pre_save_embedded_models()
        self.pre_save()
        try:
            if self._id:
                if self._changed_fields:
                    self.db.update_one({"_id": self._id}, {"$set": self.to_update_dict()})
                    self._changed_fields.clear()
            else:
                self._id = self.db.insert_one(self.to_dict()).inserted_id
                self._changed_fields.clear()
        except Exception as e:
            logger.error(f"Error al guardar el documento: {e}")

    def delete(self) -> None:
        if self._id:
            try:
                self.db.delete_one({"_id": self._id})
            except Exception as e:
                logger.error(f"Error al eliminar el documento: {e}")

    @classmethod
    def find(cls, filter: dict[str, Any]) -> 'ModelCursor':
        try:
            if "_id" in filter and isinstance(filter["_id"], str):
                filter["_id"] = ObjectId(filter["_id"])
            return ModelCursor(cls, cls.db.find(filter))
        except Exception as e:
            logger.error(f"Error al buscar documentos: {e}")
            return ModelCursor(cls, [])
    
    @classmethod
    def aggregate(cls, pipeline: list[dict]) -> 'ModelCursor':
        try:
            cursor = cls.db.aggregate(pipeline)
            return ModelCursor(cls, cursor)
        except Exception as e:
            logger.error(f"Error al realizar la agregación: {e}")
            return ModelCursor(cls, [])
        
    @classmethod
    def init_class(cls, db_collection: collection.Collection, required_vars: set[str], admissible_vars: set[str]) -> None:
        cls.db = db_collection
        cls.required_vars = required_vars
        cls.admissible_vars = admissible_vars
        cls._create_indexes()

    @classmethod
    def _create_indexes(cls):
        for field in cls.required_vars:
            cls.db.create_index([(field, pymongo.ASCENDING)])
        for field in cls._embedded_fields + cls._embedded_list_fields:
            model_class = cls._model_classes.get(field)
            if model_class == Direccion:
                cls.db.create_index([(f"{field}.location", pymongo.GEOSPHERE)])

    @staticmethod
    def _process_reference(value):
        if isinstance(value, Model):
            return value._id
        elif isinstance(value, ObjectId):
            return value
        elif isinstance(value, str):
            return ObjectId(value)
        else:
            raise ValueError("La referencia debe ser una instancia de Model, ObjectId o cadena de texto representando un ObjectId")

    def _process_embedded_list_field(self, field_name: str, value: list):
        if not isinstance(value, list):
            raise ValueError(f"{field_name} debe ser una lista")
        model_class = self._model_classes.get(field_name, Model)
        return [model_class(**item) if isinstance(item, dict) else item for item in value]

    def _process_embedded_field(self, field_name: str, value):
        model_class = self._model_classes.get(field_name, Model)
        if isinstance(value, dict):
            return model_class(**value)
        elif not isinstance(value, Model):
            raise ValueError(f"{field_name} debe ser un dict o una instancia de Model")
        return value

    def _process_reference_list_field(self, value: list):
        if not isinstance(value, list):
            raise ValueError("Las referencias deben ser una lista")
        return [self._process_reference(item) for item in value]

    def _pre_save_embedded_models(self):
        fields = self._embedded_list_fields + self._embedded_fields + self._denormalized_fields + self._denormalized_list_fields
        for field_name in fields:
            value = getattr(self, field_name, None)
            if value:
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, Model):
                            item.pre_save()
                elif isinstance(value, Model):
                    value.pre_save()

class Direccion(Model):
    required_vars = {"calle", "numero", "ciudad", "codigo_postal", "pais"}
    required_fields_order = ["calle", "numero", "portal", "piso", "codigo_postal", "ciudad", "pais"]
    admissible_vars = {"portal", "piso", "location"}
    def pre_save(self):
        if not getattr(self, 'location', None):
            address_components = [str(getattr(self, key)) for key in self.required_fields_order if getattr(self, key, None)]
            address_str = ', '.join(address_components)
            logger.info(f"Construyendo dirección para geocodificación: {address_str}")
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
    _denormalized_list_fields = ['proveedores']
    _model_classes = {'proveedores': Proveedor}

class Compra(Model):
    required_vars = {"productos", "cliente", "precio_compra", "fecha_compra", "direccion_envio"}
    admissible_vars = set()
    _embedded_fields = ['direccion_envio']
    _denormalized_fields = ['cliente']
    _denormalized_list_fields = ['productos']
    _model_classes = {'direccion_envio': Direccion, 'cliente': Cliente, 'productos': Producto}
    _date_fields = {'fecha_compra'}

class ModelCursor:
    def __init__(self, model_class: Type[Model], cursor):
        self.model_class = model_class
        self.cursor = cursor
    def __iter__(self) -> Generator[Model, None, None]:
        while True:
            try:
                yield self.model_class(**next(self.cursor))
            except StopIteration:
                break
            except Exception as e:
                logger.error(f"Error al iterar sobre el documento: {e}")
                continue

def init_app() -> None:
    client = MongoClient(URL_DB)
    db = client[DB_NAME]
    Cliente.init_class(db["cliente"], Cliente.required_vars, Cliente.admissible_vars)
    Producto.init_class(db["producto"], Producto.required_vars, Producto.admissible_vars)
    Compra.init_class(db["compra"], Compra.required_vars, Compra.admissible_vars)
    Proveedor.init_class(db["proveedor"], Proveedor.required_vars, Proveedor.admissible_vars)
