__authors__ = 'Juan-Ignacio-Diez_Unai-Puelles'
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
    geolocator = Nominatim(user_agent="My-store")
    location = geolocator.geocode(address)
    return {
        "type": "Point",
        "coordinates": [location.latitude, location.longitude]
    }


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
        self.model_class = model_class
        self.command_cursor = command_cursor

    def next(self):
        """ Devuelve el siguiente documento en forma de modelo
        """
        dictionary = self.command_cursor.next()
        return self.model_class(**dictionary)

    @property
    def alive(self):
        """True si existen más modelos por devolver, False en caso contrario
        """
        return self.command_cursor.alive()


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
    geojson_vars = []
    db = None
    validated = False
    initialized = False

    def __init__(self, vars_path, **kwargs):
        # Init class
        if not self.initialized:
            self.init_class(vars_path)

        if len(kwargs) != 0:
            self.check_vars(**kwargs)
            if self.validated:
                self.__dict__.clear()
                self.__dict__.update(kwargs)
            else:
                if '_id' in kwargs:
                    print("[ERROR] El objeto obtenido de base de datos no coincide con las variables de configuracion del modelo", self.__class__.__name__)
                else:
                    print("[ERROR] Las variables introducidas no coinciden con las variables del modelo")

    def save(self):
        if self.validated:
            if not ('_id' in self.__dict__):
                data = self.__dict__
                self.set_geo_json_data()
                self.db.insert_one(data)
                print(data)
            else:
                data_to_update = dict()
                for keys in self.updated_vars:
                    data_to_update[keys] = self.__dict__[keys]
                self.db.update_one({
                    "_id": self.__dict__["_id"]
                }, data_to_update)

    def update(self, **kwargs):
        dictionary = self.__dict__
        updated_vars_tmp = []
        for key, value in kwargs.items():
            updated_vars_tmp.append(key)
            dictionary[key] = value
        self.check_vars(**dictionary)
        if self.validated:
            self.__dict__.update(kwargs)
            self.updated_vars = updated_vars_tmp
        else:
            print("[ERROR] No actualizado: Los datos introducidos en el modelo no estan validados")

    @classmethod
    def query(cls, query):
        """ Devuelve un cursor de modelos
        """
        command_cursor = cls.db.aggregate(query)
        return ModelCursor(cls, command_cursor)

    def set_geo_json_data(self):
        if len(self.geojson_vars) != 0:
            for key in self.geojson_vars:
                if key in self.__dict__:
                    self.__dict__[key] = getCityGeoJSON(self.__dict__[key])

    @classmethod
    def init_class(cls, vars_path):
        """ Inicializa las variables de clase en la inicializacion del sistema.
        Argumentos:
            vars_path (str) -- ruta al archivo con la definicion de variables
            del modelo.
        """
        client = MongoClient('192.168.1.100', 27017)
        cls.db = client.store
        cls.read_model_vars(vars_path)
        cls.initialized = True

    @classmethod
    def read_model_vars(cls, vars_path):
        with open('model_vars/'+vars_path) as vars_file:
            vars_reader = csv.reader(vars_file, delimiter=';')
            for row in vars_reader:
                if row[1] == 'required':
                    cls.required_vars.append(row[0])
                elif row[1] == 'admissible':
                    cls.admissible_vars.append(row[0])
                if len(row) == 3:
                    if row[2] == "geojson":
                        cls.geojson_vars.append(row[0])

    @classmethod
    def check_vars(cls, **kwargs):
        kwargs_keys = set(kwargs.keys())
        # Verificamos si todas las variables de required_vars existen en el diccionario
        if kwargs_keys.issuperset(cls.required_vars):
            kwargs_keys = kwargs_keys - set(cls.required_vars) # Eliminamos las variables obligatorias ya varificadas
            kwargs_keys = kwargs_keys - set(cls.admissible_vars) # Eliminamos las variables opcionales
            if len(kwargs_keys) == 0: # Si queda alguna variable significa que no estaba delcarada en el fichero de configuracion del modelo
                cls.validated = True


class Client(MongoDBGenericModel):
    def __init__(self, **kwargs):
        super().__init__('client.vars', **kwargs)
        self.select_db_collection()

    @classmethod
    def select_db_collection(cls):
        cls.db = cls.db.cliente


class Product(MongoDBGenericModel):
    def __init__(self, **kwargs):
        super().__init__('product.vars', **kwargs)
        self.select_db_collection()

    @classmethod
    def select_db_collection(cls):
        cls.db = cls.db.producto


class Provider(MongoDBGenericModel):
    def __init__(self, **kwargs):
        super().__init__('provider.vars', **kwargs)
        self.select_db_collection()

    @classmethod
    def select_db_collection(cls):
        cls.db = cls.db.producto


class Shopping(MongoDBGenericModel):
    def __init__(self, **kwargs):
        super().__init__('shopping.vars', **kwargs)
        self.select_db_collection()

    @classmethod
    def select_db_collection(cls):
        cls.db = cls.db.producto

# Q1: Listado de todas las compras de un cliente
nombre = "Definir"
Q1 = []

# Q2: etc...

if __name__ == '__main__':
    #cliente1 = {
    #    "name": "Unai Puelles Lopez prueba 1",
    #    "billing_address": "Juntas generales 55 4C, Vitoria-Gasteiz",
    #    "payment_cards": "pruebas",
    #    "discharge_date": "pruebas",
    #    "last_access_date": "2020",
    #}
    #cliente = Cliente(**cliente1)
    #updated_vars = {"name": "Unai Puelles Lopez prueba2", "pament_cards": "pruebas2"}
    #cliente.update(**updated_vars)
    #cliente.save()
    #print(cliente)

    cliente = Client()
    model_cursor = cliente.query([{'$match': {'name': "Unai Puelles Lopez prueba 1"}}])

    clientbd1 = print(model_cursor.next())
    clientebd = model_cursor.next()
    print(clientebd)
