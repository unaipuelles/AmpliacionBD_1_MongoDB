"""
#################################################
############ PROYECTO 1: MONGO DB ###############
#################################################
"""
__authors__ = 'Juan-Ignacio-Diez_Unai-Puelles'

"""
Librerias necesarias para el programa
"""
import csv
from pymongo import MongoClient
import datetime

"""
Funcion que coge la direccion de la persona y lo transforma en coordenadas
"""


def getCityGeoJSON(address):
    """ Devuelve las coordenadas de una direccion a partir de un str de la direccion
    Argumentos:
        address (str) -- Direccion
    Return:
        (str) -- GeoJSON
    """
    from geopy.geocoders import Nominatim  # Importamos la libreria
    geolocator = Nominatim(user_agent="My-store")  # Establecemos un nombre de aplicacion
    location = geolocator.geocode(address)  # Hacemos la llamada de la direccion
    return {  # retorna las coordenadas en formato de GEOJSON
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
        dictionary = self.command_cursor.next()  # Recogemos el diccionario de la siguiente respuesta de la base de datos
        return self.model_class(**dictionary)  # Retornamos el objeto con el diccionario (desempaquetado)

    @property
    def alive(self):
        """True si existen más modelos por devolver, False en caso contrario
        """
        return self.command_cursor.alive  # Devolvemos true o false si existe el documento o no


class MongoDBGenericModel:
    """ Prototipo de la clase modelo
        Copiar y pegar tantas veces como modelos se deseen crear (cambiando
        el nombre Model, por la entidad correspondiente), o bien crear tantas
        clases como modelos se deseen que hereden de esta clase. Este segundo
        metodo puede resultar mas compleja
    """

    """
        Constructor de la clase
    """

    def __init__(self, **kwargs):
        if len(kwargs) != 0:  # Comprobamos que el diccionario no está vacia
            self.__check_vars__(**kwargs)  # Valida el diccionario con el fichero de la configuración de la clase
            if self.validated:  # Comprobamos si esta validado
                self.__dict__.update(kwargs)  # Actualiza el diccionario de la clase
            else:
                if '_id' in kwargs:  # Si no esta validado miramos si existe la id
                    print(
                        "[ERROR] El objeto obtenido de base de datos no coincide con las variables de configuracion del modelo",
                        self.__class__.__name__)
                else:  # Y si no existe la id te salta un mensaje de error
                    print("[ERROR] Las variables introducidas no coinciden con las variables del modelo")

    """
        Guardar los datos en la base de datos 
    """

    def save(self):
        if self.validated:  # Comprobamos que los datos estan validados
            if not ('_id' in self.__dict__):  # Miramos si existe el id en el diccionario
                # No existe en base de datos (insercion)
                data = self.__dict__  # Guardamos el diccionario en una variable
                self.__set_geo_json_data__()  # Generamos coordenadas para todas las variables geojson
                self.__dict__["_id"] = self.db.insert_one(
                    data)  # Guardamos los datos en base de datos y guardamos la id
            else:
                # Existe en base de datos (actualizacion)
                data_to_update = dict()
                self.__set_geo_json_data__()
                for keys in self.updated_vars:  # Recorremos las variables a actualizar
                    data_to_update[keys] = self.__dict__[
                        keys]  # Gardamos en el diccionario clave y valor de la variable

                # Actualizamos los datos mofificados en base de datos
                self.db.update_one({
                    "_id": self._id
                }, {
                    "$set": data_to_update
                })

    """
        Actualizar los datos en el objeto
    """

    def update(self, **kwargs):
        dictionary = self.__dict__  # Guardamos el diccionario en un diccionario temporal
        updated_vars_tmp = []
        for key, value in kwargs.items():  # Iteramos el diccionario con los nuevos datos
            updated_vars_tmp.append(key)  # Agregamos la key a las variables actualizadas
            dictionary[key] = value  # Cambiamos el valor en el diccionario
        self.__check_vars__(**dictionary)  # Validamos el diccionario modificado
        if self.validated:
            # Si se han validado los nuevos valores actualizamos el diccionario del objeto y las updated_vars
            self.__dict__.update(kwargs)
            self.updated_vars = updated_vars_tmp
        else:
            # Nuevos valores no validados. Mostrar mensaje de error
            print("[ERROR] No actualizado: Los datos introducidos en el modelo no estan validados")

    """
        Generar geojson para las variables necesarias
    """

    def __set_geo_json_data__(self):
        if len(self.geojson_vars) != 0:  # Verificamos si hay variables configuradas como geojson
            for key in self.geojson_vars:  # Iteramos las variables configuradas
                if key in self.__dict__:  # Miramos si existe la variable en el diccionario (por si la variable es admisible)
                    if "position" in self.__dict__[key]:
                        # Modificamos solo las coordenadas
                        self.__dict__[key]["position"] = getCityGeoJSON(self.__dict__[key]["literal"])
                    else:
                        # Generamos una estructura con la direccion en string y las coordenadas
                        self.__dict__[key] = {
                            "literal": self.__dict__[key],
                            "position": getCityGeoJSON(self.__dict__[key])
                        }

    """
        Comprobar variables del diccionario con las variables configuradas para el objeto 
    """

    def __check_vars__(self, **kwargs):
        kwargs_keys = set(kwargs.keys())  # Convertimos las keys en un set para futuras operaciones
        # Verificamos si todas las variables de required_vars existen en el diccionario
        if kwargs_keys.issuperset(self.required_vars):
            kwargs_keys = kwargs_keys - set(self.required_vars)  # Eliminamos las variables obligatorias ya verificadas
            kwargs_keys = kwargs_keys - set(self.admissible_vars)  # Eliminamos las variables opcionales
            if len(
                    kwargs_keys) == 0:  # Si queda alguna variable significa que no estaba delcarada en el fichero de configuracion del modelo
                self.validated = True

    """
        Consultas a base de datos
    """

    @classmethod
    def query(cls, query):
        """ Devuelve un cursor de modelos
        """
        command_cursor = cls.db.aggregate(query)  # Realizar la consulta aggregate a base de datos y guardar el cursor
        return ModelCursor(cls, command_cursor)  # Retornamos un cursor especifico del modelo

    """
        Inicializar las variables de la clase
    """

    @classmethod
    def init_class(cls, db, vars_path):
        """ Inicializa las variables de clase en la inicializacion del sistema.
        Argumentos:
            vars_path (str) -- ruta al archivo con la definicion de variables
            del modelo.
        """
        cls.required_vars = []
        cls.admissible_vars = []
        cls.updated_vars = []
        cls.geojson_vars = []
        cls.db = db
        cls.validated = False
        cls.__read_model_vars__(vars_path)  # Cargar variables del modelo

    """
        Leer fichero de configuracion de las variables del modelo
    """

    @classmethod
    def __read_model_vars__(cls, vars_path):
        with open('model_vars/' + vars_path) as vars_file:  # Abrimos el fichero
            vars_reader = csv.reader(vars_file, delimiter=';')  # Lo abrimos como csv con separador ;
            for row in vars_reader:  # Iteramos cada linea
                if row[1] == 'required':
                    cls.required_vars.append(row[0])  # Si contiene required lo agregamos a required_vars
                elif row[1] == 'admissible':
                    cls.admissible_vars.append(row[0])  # Si contiene admissible lo agregamos a admissible_vars
                if len(row) == 3:
                    if row[2] == "geojson":
                        cls.geojson_vars.append(row[0])  # Si la variable contiene geojson agregamos a geojson_vars


"""
    Clase cliente que hereda de MongoDBGenericModel, heredando de la misma manera las siguientes clases.
"""
class Client(MongoDBGenericModel):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)  # Heredamos metodos y variables de la clase padre


class Product(MongoDBGenericModel):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class Provider(MongoDBGenericModel):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class Shopping(MongoDBGenericModel):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


# Q1: Listado de todas las compras de un cliente
client_name_Q1 = "Aitor Sancho Martínez"
Q1 = [{
    "$match": {"client.name": client_name_Q1}
}
]

# Q2: Listado de todos los proveedores para un producto
product_name_Q2 = "The last of us 2"
Q2 = [{
    "$match": {"name": product_name_Q2}
},
    {"$group": {"_id": "$providers.name"}}
]

# Q3: Listado de todos los productos diferentes comprados por un cliente
client_name_Q3 = "Aitor Sancho Martínez"
# db.product.aggregate(Q3)
Q3 = [
    {
        '$match': {"client.name": client_name_Q3}
    },
    {
        '$group': {
            '_id': '$client._id',
            'products': {'$push': '$products.name'}
        }
    }
]

# Q4: Calcular  el  peso  y  volumen  total  de  los  productos comprados  por  un  cliente  un  día determinado
client_name_Q4 = "Aitor Sancho Martínez"
date_from = datetime.datetime(2020, 10, 23, 0, 0, 0)
date_to = datetime.datetime(2020, 10, 28, 0, 0, 0)
Q4 = [
    {
        "$match": {
            "client.name": client_name_Q4,
            "date_of_purchase": {
                "$gte": date_from,
                "$lt": date_to
            }
        }
    },
    {"$unwind": "$products"},
    {
        "$lookup": {
            "from": "product",
            "localField": "products._id",
            "foreignField": "_id",
            "as": "product"
        }},
    {
        "$project": {
            "dimensions": {"$sum": "$product.dimensions"},
            "weight": {"$sum": "$product.weight"}
        }
    },
    {"$group": {
        "_id": "$client._id",
        "dimensionsTotal": {"$sum": "$dimensions"},
        "weightTotal": {"$sum": "$weight"},
    }}
]

# Q5: Calcular el número medio de envíos por mes y almacén
Q5 = [
    {"$unwind": "$products"},
    {"$unwind": "$products.providers"},
    {"$project": {
        "month": {
            "$month": "$date_of_purchase"
        },
        "almacen": "$products.providers.name"
    }},
    {"$group": {
        "_id": ["$month", "$almacen"],
        "count": {"$sum": 1}
    }},
    {"$group": {
        "_id": {"$last": "$_id"},
        "avg": {"$avg": "$count"}
    }}
]

# Q6: Listado con los tres proveedores con más volumen de facturación. Mostrar proveedor y volumen de facturación
Q6 = [
    {
        "$project": {"products.providers.name": 1}
    },
    {"$unwind": "$products"},
    {
        "$group": {
            "_id": "$products.providers.name",
            "count": {"$sum": 1}
        }
    },
    {
        "$sort": {"count": -1}
    },
    {"$limit": 3}
]

# Q7: Listado  de almacenescerca  de  unas  coordenadas  determinadas  (100km  de  distancia máxima) ordenadas por
# orden de distancia
coordinates = [40.4523607, -3.8050322]
# Indexar primero las coordenadas db.provider.createIndex({"store_address.position.coordinates": "2dsphere"})
# Recordar poner .sort({"store_address.position":1}) para ordenar por la direccion
Q7 = {
    "store_address.position":
        {
            "$near":
                {
                    "$geometry": {"type": "Point", "coordinates": coordinates},
                    "$maxDistance": 100
                }
        }
}

# Q8: Listado  de compras  con  destino dentro  de  un  polígono  cuyos vérticesvienen  definidos por coordenadas
polygon = [42.853353, -2.672246], [42.846887, -2.676387], [42.847674, -2.666173]
# Indexar las coordenadas: db.shopping.ensureIndex({"client.shipping_adress.position":"2d"})
# cursor = db['shopping'].find(Q8)
# Google earth con el archivo kml para visualizar el poligono
Q8 = {
    "client.shipping_address.position":
        {
            "$within": {
                "$polygon": polygon
            }
        }
}

"""
    Inicializar datos por defecto en la base de datos
"""
def initialize_db_data():
    client1_data = {
        "name": "Aitor Sancho Martínez",
        "billing_address": "Calle cuchilleria 32, Vitoria-Gasteiz",
        "payment_cards": [
            {
                "name": "Aitor Sancho Martínez",
                "number": 4546458745894562,
                "expire_date": "08/23",
                "cvv": 78
            }
        ],
        "discharge_date": datetime.datetime(2002, 10, 27, 0, 0, 0),
        "last_access_date": datetime.datetime.utcnow()
    }
    client2_data = {
        "name": "Unai Puelles López",
        "billing_address": "Calle rosa de lima 4, Las rozas de Madrid",
        "shipping_address": "Calle zapateria 2, Vitoria-Gasteiz",
        "payment_cards": [
            {
                "name": "Unai Puelles López",
                "number": 4546458745894562,
                "expire_date": "08/21",
                "cvv": 54
            },
            {
                "name": "Unai Puelles López",
                "number": 4543568974851245,
                "expire_date": "08/21",
                "cvv": 52
            }
        ],
        "discharge_date": datetime.datetime(2010, 5, 1, 0, 0, 0),
        "last_access_date": datetime.datetime.utcnow()
    }
    client3_data = {
        "name": "Juan Ignacio Diez",
        "billing_address": "Calle dato 12, Vitoria-Gasteiz",
        "payment_cards": [
            {
                "name": "Aitor Sancho Martínez",
                "number": "4546458745894562",
                "expire_date": "08/23",
                "cvv": "078"
            }
        ],
        "discharge_date": datetime.datetime(2002, 10, 27, 0, 0, 0),
        "last_access_date": datetime.datetime.utcnow()
    }
    client1 = Client(**client1_data)
    client2 = Client(**client2_data)
    client3 = Client(**client3_data)
    client1.save()
    client2.save()
    client3.save()

    # Insertar proveedores
    provider1_data = {
        "name": "Metick solutions",
        "store_address": "Camino del tomillaron, Las Rozas de Madrid"
    }
    provider2_data = {
        "name": "Naughty dog",
        "store_address": "Calle Gómez Tejedor, Pozuelo"
    }
    provider3_data = {
        "name": "Insomiac Games",
        "store_address": "Portal de foronda, Vitoria-Gasteiz"
    }
    provider1 = Provider(**provider1_data)
    provider2 = Provider(**provider2_data)
    provider3 = Provider(**provider3_data)
    provider1.save()
    provider2.save()
    provider3.save()

    # Insertar productos
    product1_data = {
        "name": "The last of us 2",
        "code": 1,
        "prize_original": 41.31,
        "prize_tax": 49.99,
        "shipment_prize": 7.15,
        "discount_by_date_range": 4,
        "dimensions": 0.3,
        "weight": 4,
        "providers": [
            {
                "_id": provider1._id.inserted_id,
                "name": provider1.name
            },
            {
                "_id": provider3._id.inserted_id,
                "name": provider3.name
            }
        ],
        "stock": 10,
        "description": "Juego de consola"
    }
    product2_data = {
        "name": "Red dead redemption 2",
        "code": 2,
        "prize_original": 57.84,
        "prize_tax": 69.99,
        "shipment_prize": 3.99,
        "discount_by_date_range": 3,
        "dimensions": 0.3,
        "weight": 4,
        "providers": {
            "_id": provider2._id.inserted_id,
            "name": provider2.name
        },
        "stock": 5,
        "description": "Juego de consola"
    }
    product3_data = {
        "name": "Grand Theft Auto V",
        "code": 3,
        "prize_original": 13.18,
        "prize_tax": 15.95,
        "shipment_prize": 3.99,
        "discount_by_date_range": 3,
        "dimensions": 0.3,
        "weight": 4,
        "providers": {
            "_id": provider3._id.inserted_id,
            "name": provider3.name
        },
        "stock": 3,
        "description": "Juego de consola"
    }
    product1 = Product(**product1_data)
    product2 = Product(**product2_data)
    product3 = Product(**product3_data)
    product1.save()
    product2.save()
    product3.save()

    # Insertar compras
    shopping1_data = {
        "products": [
            {
                "_id": product1._id.inserted_id,
                "name": product1.name,
                "prize_tax": product1.prize_tax,
                "providers": {
                    "_id": provider1._id.inserted_id,
                    "name": provider1.name
                }
            },
            {
                "_id": product2._id.inserted_id,
                "name": product2.name,
                "prize_tax": product2.prize_tax,
                "providers": {
                    "_id": provider2._id.inserted_id,
                    "name": provider2.name
                }
            },
        ],
        "client": {
            "_id": client1._id.inserted_id,
            "name": client1.name,
            "shipping_address": {
                "literal": client1.billing_address['literal'],
                "position": client1.billing_address['position']
            }
        },
        "purchase_price": product1.prize_tax + product2.prize_tax,
        "date_of_purchase": datetime.datetime(2020, 10, 23, 12, 31, 53)
    }
    shopping2_data = {
        "products": [
            {
                "_id": product3._id.inserted_id,
                "name": product3.name,
                "prize_tax": product3.prize_tax,
                "providers": {
                    "_id": provider3._id.inserted_id,
                    "name": provider3.name
                }
            },
        ],
        "client": {
            "_id": client2._id.inserted_id,
            "name": client2.name,
            "shipping_address": {
                "literal": client2.shipping_address['literal'],
                "position": client2.shipping_address['position']
            }
        },
        "purchase_price": product3.prize_tax,
        "date_of_purchase": datetime.datetime(2020, 10, 26, 17, 16, 32)
    }
    shopping3_data = {
        "products": [
            {
                "_id": product3._id.inserted_id,
                "name": product3.name,
                "prize_tax": product3.prize_tax,
                "providers": {
                    "_id": provider3._id.inserted_id,
                    "name": provider3.name
                }
            }
        ],
        "client": {
            "_id": client1._id.inserted_id,
            "name": client1.name,
            "shipping_address": {
                "literal": client1.billing_address['literal'],
                "position": client1.billing_address['position']
            }
        },
        "purchase_price": product3.prize_tax,
        "date_of_purchase": datetime.datetime(2020, 10, 27, 13, 00, 40)
    }

    shopping1 = Shopping(**shopping1_data)
    shopping2 = Shopping(**shopping2_data)
    shopping3 = Shopping(**shopping3_data)
    shopping1.save()
    shopping2.save()
    shopping3.save()


if __name__ == '__main__':
    # Crear la conexion a base de datos
    mongoClient = MongoClient('192.168.1.100', 27017)
    db = mongoClient.store

    # Inicializar todas las clases con sus respectivos atributos y la coleccion de base de datos
    Client().init_class(db['client'], 'client.vars')
    Product().init_class(db['product'], 'product.vars')
    Provider().init_class(db['provider'], 'provider.vars')
    Shopping().init_class(db['shopping'], 'shopping.vars')

    # Descomentar para insertar datos por defecto
    # initialize_db_data()

    ## Modificar el nombre de un cliente
    clientCursor = Client.query([{'$match': {'name': "Aitor Sancho Martínez"}}])  # Realizamos la query en base de datos
    # Miramos si se ha encontrado algun cliente
    if clientCursor.alive:  # Bucle mientras se sigan encontrando documentos
        client = clientCursor.next()  # Recogemos la siguiente Clase
        print(client.name)
        newName = {"name": "Aitor Sancho Martinez"}
        client.update(**newName)  # Actualizamos el nombre
        client.save()  # Guardamos los nuevos datos en base de datos

    # Query 1
    cursor = Shopping().query(Q1)
    while cursor.alive:
        shopping = cursor.next()
        print("[Q1] Compra: ", shopping.date_of_purchase)

    # Query 2
    cursor = db['product'].aggregate(Q2)
    while cursor.alive:
        print("[Q2]", cursor.next())

    # Query 3
    cursor = db['shopping'].aggregate(Q3)
    while cursor.alive:
        print("[Q3]", cursor.next())

    # Query 4
    cursor = db['shopping'].aggregate(Q4)
    while cursor.alive:
        print("[Q4]", cursor.next())

    # Query 5
    cursor = db['shopping'].aggregate(Q5)
    while cursor.alive:
        print("[Q5]", cursor.next())

    # Query 6
    cursor = db['shopping'].aggregate(Q6)
    while cursor.alive:
        print("[Q6]", cursor.next())

    # Query 7. Indexar primero: db.provider.createIndex({"store_address.position.coordinates": "2dsphere"})
    cursor = db['provider'].find(Q7).sort([("store_address.position",1)])
    while cursor.alive:
        print("[Q7]", cursor.next())

    # Query 8. Indexar primero: db.shopping.ensureIndex({"client.shipping_adress.position":"2d"})
    cursor = db['shopping'].find(Q8)
    while cursor.alive:
        print("[Q8]", cursor.next())
