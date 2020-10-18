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
        return self.command_cursos.alive()
        #TODO
        # this.atributo = atributo
        self.model_class = model_class
        self.command_cursor = command_cursor




        #pass #No olvidar eliminar esta linea una vez implementado

    def next(self):
        """ Devuelve el siguiente documento en forma de modelo
        """
        #TODO

        diccionario = self.command_cursor.next()
        return self.model_class(**diccionario)

        print("next..")




        #pass #No olvidar eliminar esta linea una vez implementado

    @property
    def alive(self):
        """True si existen más modelos por devolver, False en caso contrario
        """
        #TODO
        self.command_cursor.alive()

        #pass #No olvidar eliminar esta linea una vez implementado


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

    def __init__(self, vars_path, **kwargs):
        self.init_class(vars_path)
        self.check_vars(**kwargs)
        if self.validated:
            self.__dict__.update(kwargs)
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
        self.__dict__.update(kwargs)
        for key in kwargs.keys():
            self.updated_vars.append(key)
            
    @classmethod
    def query(cls, query):
        """ Devuelve un cursor de modelos        
        """ 
        #TODO
        # cls() es el puntero a la clase
        command_cursor = cls.db.aggregate(query)
        return ModelCursor(cls.__class__, command_cursor)

        #pass #No olvidar eliminar esta linea una vez implementado

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
        client = MongoClient('127.0.0.1', 27017)
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


class Cliente(MongoDBGenericModel):
    def __init__(self, **kwargs):
        super().__init__('cliente.vars', **kwargs)
        self.select_db_collection()

    @classmethod
    def select_db_collection(cls):
        cls.db = cls.db.cliente


class Producto(MongoDBGenericModel):
    def __init__(self, **kwargs):
        super().__init__('producto.vars', **kwargs)
        self.select_db_collection()

    @classmethod
    def select_db_collection(cls):
        cls.db = cls.db.producto

# Q1: Listado de todas las compras de un cliente


nombre = "Definir"
Q1 = []



# Q2: etc...

if __name__ == '__main__':
    cliente1 = {
        "_id": "1",
        "name": "Unai Puelles Lopez prueba 1",
        "billing_address": "Juntas generales 55 4C, Vitoria-Gasteiz",
        "payment_cards": "pruebas",
        "discharge_date": "pruebas",
        "last_access_date": "2020",
    }

    cliente = Cliente(**cliente1)
    updated_vars = {"name": "Unai Puelles Lopez prueba2", "pament_cards": "pruebas2"}
    cliente.update(**updated_vars)
    cliente.save()

    print(cliente)

