from neo4j import GraphDatabase

# Credenciales de conexión a Neo4j
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "testtest"

class DBInitializer:
    def __init__(self, uri, user, password):
        # Conexión al servidor Neo4j
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Cierre de conexión
        self.driver.close()

    def initialize_infrastructure(self):
        # Ciudades clasificadas
        almacenes = ["Madrid", "Barcelona", "Valencia"]
        plataformas = ["Zaragoza", "Sevilla", "Bilbao"]
        entregas = ["Malaga", "Granada", "Valladolid", "Alicante", "Santander"]

        # Rutas según el test:
        # Tipo 1: Madrid->Malaga (Aéreo, 430km)
        # También Madrid->Granada (Carretera, 100km) utilizada en el test de manage_package
        # Tipo 2: Barcelona->Alicante (Ferrocarril, 200km)
        # Tipo 3: Valencia->Zaragoza (100km, Carretera) y Zaragoza->Santander (100km Carretera)
        # Y fallback: Madrid->Valladolid (Carretera,70km)
        rutas = [
            ("Madrid", "Malaga", "Aéreo", 430),
            ("Madrid", "Granada", "Carretera", 100),
            ("Barcelona", "Alicante", "Ferrocarril", 200),
            ("Valencia", "Zaragoza", "Carretera", 100),
            ("Zaragoza", "Santander", "Carretera", 100),
            ("Madrid", "Valladolid", "Carretera", 70)
        ]

        with self.driver.session() as session:
            # Limpieza de la base de datos
            session.run("MATCH (n) DETACH DELETE n")

            # Creación de nodos de ciudades
            for a in almacenes:
                session.run("CREATE (:Almacen {name:$name})", name=a)
            for p in plataformas:
                session.run("CREATE (:Plataforma {name:$name})", name=p)
            for e in entregas:
                session.run("CREATE (:Entrega {name:$name})", name=e)

            # Crear la estructura de rutas:
            # (Ciudad)-[:SEGMENT]->(RouteSegment {distancia_km, transporte})-[:SEGMENT]->(Ciudad)
            # Esto permite shortestPath sin problemas de sintaxis ni direccionalidad.
            for start, end, transporte, dist in rutas:
                session.run("""
                    MATCH (a {name:$start}), (b {name:$end})
                    CREATE (a)-[:SEGMENT]->(rs:RouteSegment {
                        distancia_km:$dist,
                        transporte:$transporte
                    })-[:SEGMENT]->(b)
                """, start=start, end=end, transporte=transporte, dist=dist)

            print("Infraestructura inicializada según los requisitos del test.")

if __name__ == "__main__":
    db_init = DBInitializer(URI, USER, PASSWORD)
    db_init.initialize_infrastructure()
    db_init.close()
    print("Inicialización completada.")
