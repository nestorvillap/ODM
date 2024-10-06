__author__ = 'Pablo Ramos Criado'
__students__ = 'Nestor Villa Perez y Nicolas Fernandez Perez'

from typing import Generator, Any
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.command_cursor import CommandCursor
import pymongo
from typing import Self

from config import URL_DB, DB_NAME

class Model:
    required_vars: set[str]
    admissible_vars: set[str]
    db: pymongo.collection.Collection

    def __init__(self, **kwargs: dict[str, str | dict]):
        # Manejar el campo _id de manera especial
        self._id = kwargs.pop('_id', None)
        
        # Validar los atributos antes de asignarlos
        self.validate_attributes(kwargs)
        self.__dict__.update(kwargs)

    def validate_attributes(self, attributes: dict[str, str | dict]) -> None:
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

    def __setattr__(self, name: str, value: str | dict) -> None:
        if name in self.required_vars or name in self.admissible_vars or name == '_id':
            self.__dict__[name] = value
        else:
            raise AttributeError(f"No se puede asignar una variable no admitida: {name}")

    def to_dict(self) -> dict:
        """
        Retorna un diccionario con los atributos del modelo que deben
        ser almacenados en la base de datos, excluyendo aquellos como '_id'.
        """
        doc = {k: v for k, v in self.__dict__.items() if k != '_id'}
        return doc

    def save(self) -> None:
        try:
            if self._id is not None:
                self.db.update_one({"_id": self._id}, {"$set": self.to_dict()})
            else:
                result = self.db.insert_one(self.to_dict())
                self._id = result.inserted_id
        except pymongo.errors.PyMongoError as e:
            print(f"Error al guardar el documento: {e}")

    def delete(self) -> None:
        if self._id is not None:
            try:
                self.db.delete_one({"_id": self._id})
            except pymongo.errors.PyMongoError as e:
                print(f"Error al eliminar el documento: {e}")

    @classmethod
    def find(cls, filter: dict[str, str | dict]) -> Any:
        try:
            return ModelCursor(cls, cls.db.find(filter))
        except pymongo.errors.PyMongoError as e:
            print(f"Error al buscar documentos: {e}")
            return ModelCursor(cls, [])

    @classmethod
    def aggregate(cls, pipeline: list[dict]) -> CommandCursor:
        try:
            return cls.db.aggregate(pipeline)
        except pymongo.errors.PyMongoError as e:
            print(f"Error en la consulta aggregate: {e}")
            return CommandCursor(None, [])
    
    @classmethod
    def find_by_id(cls, id: str) -> Self | None:
        try:
            document = cls.db.find_one({"_id": id})
            if document:
                return cls(**document)
            return None
        except pymongo.errors.PyMongoError as e:
            print(f"Error al buscar por ID: {e}")
            return None

    @classmethod
    def init_class(cls, db_collection: pymongo.collection.Collection, required_vars: set[str], admissible_vars: set[str]) -> None:
        cls.db = db_collection
        cls.required_vars = required_vars
        cls.admissible_vars = admissible_vars
        # Crear índice para acelerar las búsquedas por variables comunes
        for field in cls.required_vars:
            cls.db.create_index([(field, pymongo.ASCENDING)])

class Cliente(Model):
    required_vars = {"nombre", "fecha_alta"}
    admissible_vars = {"direcciones", "tarjetas_pago", "fecha_ultimo_acceso"}

class Producto(Model):
    required_vars = {"nombre", "codigo_producto_proveedor", "precio", "dimensiones", "peso", "proveedores"}
    admissible_vars = {"coste_envio", "descuento_rango_fechas"}

class Compra(Model):
    required_vars = {"productos", "cliente", "precio_compra", "fecha_compra", "direccion_envio"}
    admissible_vars = set()

class Proveedor(Model):
    required_vars = {"nombre"}
    admissible_vars = {"direcciones_almacenes"}

class ModelCursor:

    def __init__(self, model_class: Model, cursor: pymongo.cursor.Cursor):
        self.model = model_class
        self.cursor = cursor
    
    def __iter__(self) -> Generator:
        for document in self.cursor:
            yield self.model(**document)
    
    def count(self) -> int:
        try:
            return self.cursor.count()
        except pymongo.errors.PyMongoError as e:
            print(f"Error al contar documentos: {e}")
            return 0

def initApp() -> None:
    client = MongoClient(URL_DB)
    
    db = client[DB_NAME]

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
