__author__ = 'Pablo Ramos Criado'
__students__ = 'Nestor Villa Perez y Nicolas Fernandez Perez'

from typing import Any, Type, Generator, Union
from pymongo import MongoClient
from pymongo.collection import Collection
import pymongo
from bson import ObjectId
from config import URL_DB, DB_NAME

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable
import time
from geojson import Point
import logging

# Configurar el registro
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_location_point(address: str) -> Point:
    geolocator = Nominatim(user_agent="MiAplicacion/1.0 (tu_email@ejemplo.com)")
    location = None
    retry_count = 0
    max_retries = 5
    while location is None and retry_count < max_retries:
        try:
            logger.info(f"Geocodificando dirección: {address}")
            time.sleep(1)  # Respetar los límites de tasa
            location = geolocator.geocode(address)
        except (GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable) as e:
            retry_count += 1
            logger.warning(f"Error al geocodificar: {e}. Reintentando ({retry_count}/{max_retries})...")
            time.sleep(2 ** retry_count)  # Espera exponencial
        except Exception as e:
            logger.error(f"Error inesperado al geocodificar: {e}")
            break
    if location:
        logger.info(f"Coordenadas encontradas: ({location.latitude}, {location.longitude})")
        return Point((location.longitude, location.latitude))
    else:
        logger.warning(f"No se pudo obtener la ubicación para la dirección: {address}")
        return None

class Model:
    """
    Clase base para todos los modelos.
    Proporciona métodos comunes para la manipulación y persistencia de datos.
    """
    required_vars: set[str] = set()
    admissible_vars: set[str] = set()
    db: pymongo.collection.Collection = None

    # Campos para modelos anidados y referencias
    _embedded_list_fields: list[str] = []
    _embedded_fields: list[str] = []
    _reference_list_fields: list[str] = []
    _reference_fields: list[str] = []
    _model_classes: dict[str, Type['Model']] = {}

    def __init__(self, **kwargs: Any):
        # Manejar el campo _id de manera especial
        self._id = kwargs.pop('_id', None)

        # Inicializar el diccionario para seguimiento de cambios
        self._changed_fields = set()

        # Procesar y asignar los atributos
        self._process_and_set_attributes(kwargs)

    def _process_and_set_attributes(self, attributes: dict):
        """
        Procesa los atributos, manejando modelos anidados y referencias, y los asigna al objeto.
        """
        # Procesar campos de modelos anidados en listas
        for field_name in self._embedded_list_fields:
            if field_name in attributes:
                attributes[field_name] = self._process_embedded_list_field(field_name, attributes[field_name])

        # Procesar campos de modelos anidados individuales
        for field_name in self._embedded_fields:
            if field_name in attributes:
                attributes[field_name] = self._process_embedded_field(field_name, attributes[field_name])

        # Procesar campos de referencias en listas
        for field_name in self._reference_list_fields:
            if field_name in attributes:
                attributes[field_name] = self._process_reference_list_field(attributes[field_name])

        # Procesar campos de referencias individuales
        for field_name in self._reference_fields:
            if field_name in attributes:
                attributes[field_name] = self._process_reference(attributes[field_name])

        # Validar los atributos antes de asignarlos
        self.validate_attributes(attributes)

        # Asignar los atributos al objeto
        for key, value in attributes.items():
            self.__dict__[key] = value

    def validate_attributes(self, attributes: dict[str, Any]) -> None:
        """
        Valida que los atributos proporcionados sean correctos de acuerdo
        con las variables requeridas y admitidas.
        """
        missing_vars = self.required_vars - attributes.keys()
        if missing_vars:
            raise ValueError(f"Faltan variables requeridas: {missing_vars}")

        invalid_vars = attributes.keys() - (self.required_vars | self.admissible_vars)
        if invalid_vars:
            raise ValueError(f"Variables no admitidas: {invalid_vars}")

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith('_'):
            # Atributos internos
            super().__setattr__(name, value)
            return

        if name in self.required_vars or name in self.admissible_vars:
            # Marcar el campo como modificado
            if hasattr(self, name) and getattr(self, name) != value:
                self._changed_fields.add(name)
            elif not hasattr(self, name):
                self._changed_fields.add(name)

            # Asignar el valor
            self.__dict__[name] = value
        else:
            raise AttributeError(f"No se puede asignar una variable no admitida: {name}")

    def to_dict(self) -> dict:
        """
        Convierte el objeto en un diccionario listo para ser almacenado en la base de datos.
        """
        doc = {}
        for k, v in self.__dict__.items():
            if k.startswith('_'):
                continue
            if isinstance(v, list):
                doc[k] = [item.to_dict() if hasattr(item, 'to_dict') else item for item in v]
            elif hasattr(v, 'to_dict'):
                doc[k] = v.to_dict()
            else:
                doc[k] = v
        return doc

    def to_update_dict(self) -> dict:
        """
        Convierte solo los campos modificados en un diccionario para actualización parcial.
        """
        update_doc = {}
        for field in self._changed_fields:
            value = getattr(self, field)
            if isinstance(value, list):
                update_doc[field] = [item.to_dict() if hasattr(item, 'to_dict') else item for item in value]
            elif hasattr(value, 'to_dict'):
                update_doc[field] = value.to_dict()
            else:
                update_doc[field] = value
        return update_doc

    def pre_save(self):
        """
        Método que se puede sobrescribir en subclases para realizar acciones antes de guardar.
        """
        pass

    def save(self) -> None:
        """
        Guarda el objeto en la base de datos.
        Si el objeto ya existe (_id no es None), realiza una actualización parcial.
        """
        # Procesar modelos anidados antes de guardar
        self._pre_save_embedded_models()
        self.pre_save()

        # Guardar el modelo actual
        try:
            if self._id is not None:
                if self._changed_fields:
                    update_doc = self.to_update_dict()
                    self.db.update_one({"_id": self._id}, {"$set": update_doc})
                    self._changed_fields.clear()
            else:
                result = self.db.insert_one(self.to_dict())
                self._id = result.inserted_id
                self._changed_fields.clear()
        except pymongo.errors.PyMongoError as e:
            print(f"Error al guardar el documento: {e}")

    def delete(self) -> None:
        """
        Elimina el objeto de la base de datos.
        """
        if self._id is not None:
            try:
                self.db.delete_one({"_id": self._id})
            except pymongo.errors.PyMongoError as e:
                print(f"Error al eliminar el documento: {e}")

    @classmethod
    def find(cls, filter: dict[str, Any]) -> 'ModelCursor':
        """
        Busca documentos en la base de datos que coincidan con el filtro proporcionado.
        Devuelve un objeto ModelCursor que es un iterador de modelos.
        """
        try:
            cursor = cls.db.find(filter)
            return ModelCursor(cls, cursor)
        except pymongo.errors.PyMongoError as e:
            print(f"Error al buscar documentos: {e}")
            return ModelCursor(cls, [])

    @classmethod
    def find_by_id(cls, id: str) -> Union['Model', None]:
        """
        Busca un documento por su _id.
        """
        try:
            document = cls.db.find_one({"_id": ObjectId(id)})
            if document:
                return cls(**document)
            return None
        except pymongo.errors.PyMongoError as e:
            print(f"Error al buscar por ID: {e}")
            return None

    @classmethod
    def init_class(cls, db_collection: Collection, required_vars: set[str], admissible_vars: set[str]) -> None:
        """
        Inicializa la clase del modelo con la colección de la base de datos y las variables requeridas y admitidas.
        """
        cls.db = db_collection
        cls.required_vars = required_vars
        cls.admissible_vars = admissible_vars

        # Crear índices según sea necesario
        cls._create_indexes()

    @classmethod
    def _create_indexes(cls):
        """
        Crea índices en la base de datos para optimizar consultas.
        """
        # Índice para campos requeridos
        for field in cls.required_vars:
            cls.db.create_index([(field, pymongo.ASCENDING)])

        # Índices geoespaciales para campos de direcciones
        for field in cls._embedded_fields + cls._embedded_list_fields:
            model_class = cls._model_classes.get(field)
            if model_class == Direccion:
                cls.db.create_index([(f"{field}.location", pymongo.GEOSPHERE)])

    @staticmethod
    def _process_reference(value):
        """
        Procesa una referencia a otro documento, convirtiéndola en ObjectId si es necesario.
        """
        if isinstance(value, Model):
            return value._id
        elif isinstance(value, ObjectId):
            return value
        elif isinstance(value, str):
            return ObjectId(value)
        else:
            raise ValueError("La referencia debe ser una instancia de Model, ObjectId o cadena de texto representando un ObjectId")

    def _process_embedded_list_field(self, field_name: str, value: list):
        """
        Procesa un campo que es una lista de modelos anidados.
        """
        if not isinstance(value, list):
            raise ValueError(f"{field_name} debe ser una lista")
        model_class = self._model_classes.get(field_name, Model)
        processed_value = []
        for item in value:
            if isinstance(item, dict):
                item = model_class(**item)
            elif not isinstance(item, Model):
                raise ValueError(f"Los elementos en {field_name} deben ser dict o instancias de Model")
            processed_value.append(item)
        return processed_value

    def _process_embedded_field(self, field_name: str, value):
        """
        Procesa un campo que es un modelo anidado individual.
        """
        model_class = self._model_classes.get(field_name, Model)
        if isinstance(value, dict):
            value = model_class(**value)
        elif not isinstance(value, Model):
            raise ValueError(f"{field_name} debe ser un dict o una instancia de Model")
        return value

    def _process_reference_list_field(self, value: list):
        """
        Procesa un campo que es una lista de referencias a otros documentos.
        """
        if not isinstance(value, list):
            raise ValueError("Las referencias deben ser una lista")
        return [self._process_reference(item) for item in value]

    def _pre_save_embedded_models(self):
        """
        Llama al método pre_save en los modelos anidados antes de guardar.
        """
        for field_name in self._embedded_list_fields + self._embedded_fields:
            if hasattr(self, field_name):
                value = getattr(self, field_name)
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, Model):
                            item.pre_save()
                elif isinstance(value, Model):
                    value.pre_save()

class Direccion(Model):
    """
    Modelo para representar una dirección.
    """
    required_vars = {"calle", "numero", "ciudad", "codigo_postal", "pais"}
    # Lista ordenada de campos requeridos
    required_fields_order = ["calle", "numero", "portal", "piso", "codigo_postal", "ciudad", "pais"]
    admissible_vars = {"portal", "piso", "location"}

    def pre_save(self):
        if not hasattr(self, 'location') or not self.location:
            # Construir la dirección utilizando la lista ordenada
            address_components = []
            for key in self.required_fields_order:
                value = getattr(self, key, None)
                if value:
                    address_components.append(str(value))
            address_str = ', '.join(address_components)
            # Agregar un registro para depuración
            logger.info(f"Construyendo dirección para geocodificación: {address_str}")
            # Obtener el punto de ubicación
            self.location = get_location_point(address_str)

class Cliente(Model):
    """
    Modelo para representar un cliente.
    """
    required_vars = {"nombre", "fecha_alta"}
    admissible_vars = {"direcciones_facturacion", "direcciones_envio", "tarjetas_pago", "fecha_ultimo_acceso"}
    _embedded_list_fields = ['direcciones_facturacion', 'direcciones_envio']
    _model_classes = {'direcciones_facturacion': Direccion, 'direcciones_envio': Direccion}


class Proveedor(Model):
    """
    Modelo para representar un proveedor.
    """
    required_vars = {"nombre"}
    admissible_vars = {"direcciones_almacenes"}
    _embedded_list_fields = ['direcciones_almacenes']
    _model_classes = {'direcciones_almacenes': Direccion}


class Producto(Model):
    """
    Modelo para representar un producto.
    """
    required_vars = {"nombre", "codigo_producto_proveedor", "precio", "dimensiones", "peso", "proveedores"}
    admissible_vars = {"coste_envio", "descuento_rango_fechas"}
    _reference_list_fields = ['proveedores']
    _model_classes = {'proveedores': Proveedor}


class Compra(Model):
    """
    Modelo para representar una compra.
    """
    required_vars = {"productos", "cliente", "precio_compra", "fecha_compra", "direccion_envio"}
    admissible_vars = set()
    _embedded_fields = ['direccion_envio']
    _reference_fields = ['cliente']
    _reference_list_fields = ['productos']
    _model_classes = {'direccion_envio': Direccion, 'cliente': Cliente, 'productos': Producto}


class ModelCursor:
    """
    Clase para iterar sobre los resultados de una consulta y devolver objetos modelo.
    """
    def __init__(self, model_class: Type[Model], cursor):
        self.model_class = model_class
        self.cursor = cursor

    def __iter__(self) -> Generator[Model, None, None]:
        """
        Iterador que devuelve los documentos como objetos modelo.
        """
        while True:
            try:
                document = next(self.cursor)
                yield self.model_class(**document)
            except StopIteration:
                break
            except Exception as e:
                print(f"Error al iterar sobre los documentos: {e}")
                break


def init_app() -> None:
    """
    Inicializa la aplicación y las clases de modelo con la base de datos.
    """
    client = MongoClient(URL_DB)
    db = client[DB_NAME]

    # Inicializar modelos con la colección correspondiente
    Cliente.init_class(
        db_collection=db["cliente"],
        required_vars=Cliente.required_vars,
        admissible_vars=Cliente.admissible_vars
    )

    Producto.init_class(
        db_collection=db["producto"],
        required_vars=Producto.required_vars,
        admissible_vars=Producto.admissible_vars
    )

    Compra.init_class(
        db_collection=db["compra"],
        required_vars=Compra.required_vars,
        admissible_vars=Compra.admissible_vars
    )

    Proveedor.init_class(
        db_collection=db["proveedor"],
        required_vars=Proveedor.required_vars,
        admissible_vars=Proveedor.admissible_vars
    )
