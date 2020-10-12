__author__ = 'Juan-Ignacio-Diez_Unai-Puelles'
import csv
from pymongo import MongoClient

def getCityGeoJSON(address):
    """ Devuelve las coordenadas de una direcciion a partir de un str de la direccion
    Argumentos:
        address (str) -- Direccion
    Return:
        (str) -- GeoJSON
    """
    from geopy.geocoders import Nominatim
    geolocator = Nominatim()
    location = geolocator.geocode(address)
    #TODO
    # Devolver GeoJSON de tipo punto con la latitud y longitud almacenadas
    # en las variables location.latitude y location.longitude

class ModelCursor:
    """ Cursor para iterar sobre los documentos del resultado de una
    consulta. Los documentos deben ser devueltos en forma de objetos
    modelo.
    """

    def __init__(self, model_class, command_cursor):
        """ Inicializa ModelCursor
        Argumentos:
            model_class (class) -- Clase para crear los modelos del 
            documento que se itera.
            command_cursor (CommandCursor) -- Cursor de pymongo
        """
        #TODO
        pass #No olvidar eliminar esta linea una vez implementado

    def next(self):
        """ Devuelve el siguiente documento en forma de modelo
        """
        #TODO
        pass #No olvidar eliminar esta linea una vez implementado

    @property
    def alive(self):
        """True si existen más modelos por devolver, False en caso contrario
        """
        #TODO
        pass #No olvidar eliminar esta linea una vez implementado

class MongoDBGenericModel:
    """ Prototipo de la clase modelo
        Copiar y pegar tantas veces como modelos se deseen crear (cambiando
        el nombre Model, por la entidad correspondiente), o bien crear tantas
        clases como modelos se deseen que hereden de esta clase. Este segundo 
        metodo puede resultar mas compleja
    """
    required_vars = []
    admissible_vars = []
    updated_vars = []
    db = None
    validated = False

    def __init__(self, **kwargs):
        self.init_class(self)
        self.check_vars(**kwargs)
        if self.validated:
            self.__dict__.update(kwargs)
        else:
            print("[ERROR] Las variables introducidas no coinciden con las variables del modelo")

    def save(self):
        if self.validated:
            if not ('_id' in self.__dict__):
                data = self.__dict__
                print(data)
                self.db.insert_one(data)
            else:
                self.update()

    def update(self, **kwargs):
        #TODO
        pass #No olvidar eliminar esta linea una vez implementado
    
    @classmethod
    def query(cls, query):
        """ Devuelve un cursor de modelos        
        """ 
        #TODO
        # cls() es el puntero a la clase
        pass #No olvidar eliminar esta linea una vez implementado

    @classmethod
    def init_class(cls, db, vars_path='cliente.vars'):
        """ Inicializa las variables de clase en la inicializacion del sistema.
        Argumentos:
            db (MongoClient) -- Conexion a la base de datos.
            vars_path (str) -- ruta al archivo con la definicion de variables
            del modelo.
        """
        client = MongoClient('192.168.1.100', 27017)
        cls.db = client.store
        cls.read_model_vars(vars_path)

    @classmethod
    def read_model_vars(cls, vars_path):
        with open('model_vars/'+vars_path) as vars_file:
            vars_reader = csv.reader(vars_file, delimiter=';')
            for row in vars_reader:
                if row[1] == 'required':
                    cls.required_vars.append(row[0])
                elif row[1] == 'admissible':
                    cls.admissible_vars.append(row[0])

    @classmethod
    def check_vars(cls, **kwargs):
        kwargs_keys = set(kwargs.keys())
        # Verificamos si todas las variables de required_vars existen en el diccionario
        if kwargs_keys.issuperset(cls.required_vars):
            kwargs_keys = kwargs_keys - set(cls.required_vars) # Eliminamos las variables obligatorias ya varificadas
            kwargs_keys = kwargs_keys - set(cls.admissible_vars) # Eliminamos las variables opcionales
            if len(kwargs_keys) == 0: # Si queda alguna variable significa que no estaba delcarada en el fichero de configuracion del modelo
                cls.validated = True


class Cliente(MongoDBGenericModel):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.select_db_collection()

    @classmethod
    def select_db_collection(cls):
        cls.db = cls.db.cliente


class Producto(MongoDBGenericModel):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.select_db_collection()

    @classmethod
    def select_db_collection(cls):
        cls.db = cls.db.producto

# Q1: Listado de todas las compras de un cliente
nombre = "Definir"
Q1 = []

# Q2: etc...

if __name__ == '__main__':
    cliente1 = {"name": "Unai Puelles Lopez prueba 1", "billing_address": "Juntas generales", "shipping_address": "pruebas"}
    cliente = Cliente(**cliente1)
    cliente.save()

    #client = MongoClient('192.168.1.100', 27017)
    #db = client.store
    #db = db.cliente.insert_one(cliente1)
