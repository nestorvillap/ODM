__author__ = 'Pablo Ramos Criado'
__students__ = 'Nombres_y_Apellidos'


from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time
from typing import Generator, Any, Self
from geojson import Point
import pymongo
import yaml

def get_location_point(address: str) -> Point:
    """ 
    Obtiene las coordenadas de una dirección en formato geojson.Point
    Utilizar la API de geopy para obtener las coordenadas de la direccion
    Cuidado, la API es publica tiene limite de peticiones, utilizar sleeps.

    Parameters
    ----------
        address : str
            direccion completa de la que obtener las coordenadas
    Returns
    -------
        geojson.Point
            coordenadas del punto de la direccion
    """
    
    geolocator = Nominatim(user_agent="ODM/1.1 (nestorvillap@gmail.com)", timeout=10)
    for attempt in range(5):
        try:
            time.sleep(2)
            location = geolocator.geocode(address)
            if location:
                return Point((location.longitude, location.latitude))
        except:
            time.sleep(2 ** (attempt + 1))
    return None

class Model:
    """ 
    Clase de modelo abstracta
    Crear tantas clases que hereden de esta clase como  
    colecciones/modelos se deseen tener en la base de datos.

    Attributes
    ----------
        required_vars : set[str]
            conjunto de variables requeridas por el modelo
        admissible_vars : set[str]
            conjunto de variables admitidas por el modelo
        db : pymongo.collection.Collection
            conexion a la coleccion de la base de datos
    
    Methods
    -------
        __setattr__(name: str, value: str | dict) -> None
            Sobreescribe el metodo de asignacion de valores a las
            variables del objeto con el fin de controlar las variables
            que se asignan al modelo y cuando son modificadas.
        save()  -> None
            Guarda el modelo en la base de datos
        delete() -> None
            Elimina el modelo de la base de datos
        find(filter: dict[str, str | dict]) -> ModelCursor
            Realiza una consulta de lectura en la BBDD.
            Devuelve un cursor de modelos ModelCursor
        aggregate(pipeline: list[dict]) -> pymongo.command_cursor.CommandCursor
            Devuelve el resultado de una consulta aggregate.
        find_by_id(id: str) -> dict | None
            Busca un documento por su id utilizando la cache y lo devuelve.
            Si no se encuentra el documento, devuelve None.
        init_class(db_collection: pymongo.collection.Collection, required_vars: set[str], admissible_vars: set[str]) -> None
            Inicializa las variables de clase en la inicializacion del sistema.

    """
    required_vars: set[str]
    admissible_vars: set[str]
    db: pymongo.collection.Collection

    def __init__(self, **kwargs: dict[str, str | dict]):
        """
        Inicializa el modelo con los valores proporcionados en kwargs
        Comprueba que los valores proporcionados en kwargs son admitidos
        por el modelo y que las variables requeridas son proporcionadas.

        Parameters
        ----------
            kwargs : dict[str, str | dict]
                diccionario con los valores de las variables del modelo

        """

        # Realizar las comprabociones y gestiones necesarias antes de la asignacion.
        self._id = kwargs.pop('_id', None)
        self._changed_fields = set()
        self.__dict__.update(kwargs)

    def __setattr__(self, name: str, value: str | dict) -> None:
        """ 
        Sobreescribe el metodo de asignacion de valores a las 
        variables del objeto con el fin de controlar las variables
        que se asignan al modelo y cuando son modificadas.
        """

        # Realizar las comprabociones y gestiones necesarias
        # antes de la asignacion. Asigna el valor value a la variable name
        
        if name not in self.required_vars or name not in self.admissible_vars:
            return
        
        self._changed_fields[name] = value
        
    def save(self) -> None:
        """
        Guarda el modelo en la base de datos
        Si el modelo no existe en la base de datos, se crea un nuevo
        documento con los valores del modelo. En caso contrario, se
        actualiza el documento existente con los nuevos valores del
        modelo.
        """
        if self._id:
            if self._changed_fields:
                self.db.update_one( {"_id": self._id}, {"$set": self.to_update_dict()} )
                self._changed_fields.clear()
        else:
            self._id = self.db.insert_one( self.to_dict() )
            self._changed_fields.clear()

    def delete(self) -> None:
        """
        Elimina el modelo de la base de datos
        """
        if self._id:
            self.db.delete_one({"_id": self._id})
    
    @classmethod
    def find(cls, filter: dict[str, str | dict]) -> Any:
        """ 
        Utiliza el metodo find de pymongo para realizar una consulta
        de lectura en la BBDD.
        find debe devolver un cursor de modelos ModelCursor

        Parameters
        ----------
            filter : dict[str, str | dict]
                diccionario con el criterio de busqueda de la consulta
        Returns
        -------
            ModelCursor
                cursor de modelos
        """ 
        #TODO
        # cls es el puntero a la clase
        pass #No olvidar eliminar esta linea una vez implementado

    @classmethod
    def aggregate(cls, pipeline: list[dict]) -> pymongo.command_cursor.CommandCursor:
        """ 
        Devuelve el resultado de una consulta aggregate. 
        No hay nada que hacer en esta funcion.
        Se utilizara para las consultas solicitadas
        en el segundo proyecto de la practica.

        Parameters
        ----------
            pipeline : list[dict]
                lista de etapas de la consulta aggregate 
        Returns
        -------
            pymongo.command_cursor.CommandCursor
                cursor de pymongo con el resultado de la consulta
        """ 
        return cls.db.aggregate(pipeline)
    
    @classmethod
    def find_by_id(cls, id: str) -> Self | None:
        """ 
        NO IMPLEMENTAR HASTA EL TERCER PROYECTO
        Busca un documento por su id utilizando la cache y lo devuelve.
        Si no se encuentra el documento, devuelve None.

        Parameters
        ----------
            id : str
                id del documento a buscar
        Returns
        -------
            Self | None
                Modelo del documento encontrado o None si no se encuentra
        """ 
        #TODO
        pass

    @classmethod
    def init_class(cls, db_collection: pymongo.collection.Collection, required_vars: set[str], admissible_vars: set[str]) -> None:
        """ 
        Inicializa las variables de clase en la inicializacion del sistema.
        En principio nada que hacer aqui salvo que se quieran realizar
        alguna otra inicialización/comprobaciones o cambios adicionales.

        Parameters
        ----------
            db_collection : pymongo.collection.Collection
                Conexion a la collecion de la base de datos.
            required_vars : set[str]
                Set de variables requeridas por el modelo
            admissible_vars : set[str] 
                Set de variables admitidas por el modelo
        """
        cls.db = db_collection
        cls.required_vars = required_vars
        cls.admissible_vars = admissible_vars
        

class ModelCursor:
    """ 
    Cursor para iterar sobre los documentos del resultado de una
    consulta. Los documentos deben ser devueltos en forma de objetos
    modelo.

    Attributes
    ----------
        model_class : Model
            Clase para crear los modelos de los documentos que se iteran.
        cursor : pymongo.cursor.Cursor
            Cursor de pymongo a iterar

    Methods
    -------
        __iter__() -> Generator
            Devuelve un iterador que recorre los elementos del cursor
            y devuelve los documentos en forma de objetos modelo.
    """

    def __init__(self, model_class: Model, cursor: pymongo.cursor.Cursor):
        """
        Inicializa el cursor con la clase de modelo y el cursor de pymongo

        Parameters
        ----------
            model_class : Model
                Clase para crear los modelos de los documentos que se iteran.
            cursor: pymongo.cursor.Cursor
                Cursor de pymongo a iterar
        """
        self.model = model_class
        self.cursor = cursor
    
    def __iter__(self) -> Generator:
        """
        Devuelve un iterador que recorre los elementos del cursor
        y devuelve los documentos en forma de objetos modelo.
        Utilizar yield para generar el iterador
        Utilizar la funcion next para obtener el siguiente documento del cursor
        Utilizar alive para comprobar si existen mas documentos.
        """
        #TODO
        pass #No olvidar eliminar esta linea una vez implementado


def initApp(definitions_path: str = "./models.yml", mongodb_uri="mongodb://localhost:27017/", db_name="abd") -> None:
    """ 
    Declara las clases que heredan de Model para cada uno de los 
    modelos de las colecciones definidas en definitions_path.
    Inicializa las clases de los modelos proporcionando las variables 
    admitidas y requeridas para cada una de ellas y la conexión a la
    collecion de la base de datos.
    
    Parameters
    ----------
        definitions_path : str
            ruta al fichero de definiciones de modelos
        mongodb_uri : str
            uri de conexion a la base de datos
        db_name : str
            nombre de la base de datos
    """
    #TODO
    # Inicializar base de datos

    #TODO
    # Declarar tantas clases modelo colecciones existan en la base de datos
    # Leer el fichero de definiciones de modelos para obtener las colecciones
    # y las variables admitidas y requeridas para cada una de ellas.
    # Ejemplo de declaracion de modelo para colecion llamada MiModelo
    globals()["MiModelo"] = type("MiModelo", (Model,),{})
    # Ignorar el warning de Pylance sobre MiModelo, es incapaz de detectar
    # que se ha declarado la clase en la linea anterior ya que se hace
    # en tiempo de ejecucion.
    MiModelo.init_class(db_collection=None, required_vars=None, admissible_vars=None)



# TODO 
# PROYECTO 2
# Almacenar los pipelines de las consultas en Q1, Q2, etc. 
# EJEMPLO
# Q0: Listado de todas las personas con nombre determinado
nombre = "Quijote"
Q0 = [{'$match': {'nombre': nombre}}]

# Q1: 
Q2 = []

# Q2: 
Q2 = []

# Q3:
Q3 = []

# Q4: etc.


if __name__ == '__main__':
    
    # Inicializar base de datos y modelos con initApp
    #TODO
    initApp()

    #Ejemplo
    m = Model(nombre="Pablo", apellido="Ramos", edad=18)
    m.save()
    m.nombre="Pedro"
    print(m.nombre)

    # Hacer pruebas para comprobar que funciona correctamente el modelo
    #TODO
    # Crear modelo

    # Asignar nuevo valor a variable admitida del objeto 

    # Asignar nuevo valor a variable no admitida del objeto 

    # Guardar

    # Asignar nuevo valor a variable admitida del objeto

    # Guardar

    # Buscar nuevo documento con find

    # Obtener primer documento

    # Modificar valor de variable admitida

    # Guardar

    # PROYECTO 2
    # Ejecutar consultas Q1, Q2, etc. y mostrarlo
    #TODO
    #Ejemplo
    #Q1_r = MiModelo.aggregate(Q1)





