# initialize_db.py
from neo4j import GraphDatabase

# Configuración para conectarse a Neo4j
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "testtest"

class DBInitializer:
    def __init__(self, uri, user, password):
        # Conexión a la base de datos Neo4j
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Cierra la conexión
        self.driver.close()

    def initialize_infrastructure(self):
        """
        Inicializa la base de datos con nodos y relaciones representando:
        - Almacenes (puntos de origen)
        - Plataformas (puntos intermedios)
        - Entregas (puntos de destino)
        
        También crea relaciones 'RUTA' con distancia y tipo de transporte.
        """
        with self.driver.session() as session:
            # Limpiar la BD antes de comenzar
            session.run("MATCH (n) DETACH DELETE n")
            print("Base de datos limpiada.")

            # Crear nodos (Almacenes, Plataformas y Entregas)
            almacenes = [
                {"name": "Madrid", "type": "Almacen"},
                {"name": "Barcelona", "type": "Almacen"},
                {"name": "Valencia", "type": "Almacen"},
            ]

            plataformas = [
                {"name": "Zaragoza", "type": "Plataforma"},
                {"name": "Sevilla", "type": "Plataforma"},
                {"name": "Bilbao", "type": "Plataforma"},
            ]

            entregas = [
                {"name": "Malaga", "type": "Entrega"},
                {"name": "Granada", "type": "Entrega"},
                {"name": "Valladolid", "type": "Entrega"},
                {"name": "Alicante", "type": "Entrega"},
                {"name": "Santander", "type": "Entrega"},
            ]

            # Crear los nodos en la BD
            for node in almacenes + plataformas + entregas:
                session.run(f"CREATE (:{node['type']} {{name:$name}})", name=node["name"])
            print("Nodos creados: Almacenes, Plataformas y Entregas.")

            # Crear rutas con diferentes transportes y distancias
            rutas = [
                ("Madrid", "Zaragoza", "Carretera", 314),
                ("Barcelona", "Zaragoza", "Carretera", 300),
                ("Valencia", "Zaragoza", "Carretera", 309),
                ("Zaragoza", "Malaga", "Ferrocarril", 800),
                ("Zaragoza", "Granada", "Ferrocarril", 700),
                ("Zaragoza", "Santander", "Carretera", 400),
                ("Sevilla", "Malaga", "Carretera", 205),
                ("Sevilla", "Granada", "Carretera", 250),
                ("Madrid", "Malaga", "Aéreo", 430),
                ("Madrid", "Granada", "Carretera", 418),
                ("Madrid", "Valladolid", "Carretera", 193),
                ("Barcelona", "Alicante", "Ferrocarril", 535),
                ("Valencia", "Alicante", "Carretera", 125),
                ("Zaragoza", "Bilbao", "Carretera", 300),
                ("Sevilla", "Bilbao", "Marítimo", 800),
            ]

            # Crear relaciones RUTA entre los nodos
            for start, end, transporte, dist in rutas:
                session.run("""
                    MATCH (a {name:$start}), (b {name:$end})
                    CREATE (a)-[:RUTA {
                        distancia_km:$dist,
                        transporte:$transporte
                    }]->(b)
                """, start=start, end=end, transporte=transporte, dist=dist)

            print("Relaciones de rutas creadas.")

if __name__ == "__main__":
    # Inicializar la infraestructura al ejecutar este archivo
    db_initializer = DBInitializer(URI, USER, PASSWORD)
    db_initializer.initialize_infrastructure()
    db_initializer.close()
    print("Inicialización de la infraestructura completada.")
