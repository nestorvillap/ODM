import os
import uuid
from datetime import datetime, timedelta
from neo4j import GraphDatabase

# Parámetros de transporte según el enunciado
TRANSPORT_PARAMS = {
    "Carretera": {"min_100km": 60, "carga_descarga": 5, "coste_100km": 1.0},
    "Ferrocarril": {"min_100km": 50, "carga_descarga": 10, "coste_100km": 0.8},
    "Aéreo": {"min_100km": 10, "carga_descarga": 40, "coste_100km": 3.5},
    "Marítimo": {"min_100km": 120, "carga_descarga": 20, "coste_100km": 0.3},
}

class LogisticsManager:
    def __init__(self, uri, user, password):
        # Conexión a Neo4j
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Cierra la conexión
        self.driver.close()

    def get_optimal_route(self, start_name, end_name, tipo_envio):
        # Obtiene la ruta óptima usando shortestPath en un patrón City-(SEGMENT)-RouteSegment-(SEGMENT)-City
        # Sin direcciones, y con un único tipo de relación :SEGMENT
        with self.driver.session() as session:
            result = session.run("""
                MATCH (start {name:$start_name}), (end {name:$end_name})
                MATCH p = shortestPath((start)-[:SEGMENT*1..10]-(end))
                RETURN p
            """, start_name=start_name, end_name=end_name)

            candidate_paths = []
            for record in result:
                p = record["p"]
                tiempo_total, coste_total = self._calcular_tiempo_coste_ruta(p)
                # Verificar restricciones de tiempo
                if self._cumple_restricciones(tipo_envio, tiempo_total):
                    candidate_paths.append((p, tiempo_total, coste_total))

            if not candidate_paths:
                # No se encontraron rutas que cumplan restricciones
                return None

            # Ordenar por coste y devolver la mejor
            candidate_paths.sort(key=lambda x: x[2])
            best_path = candidate_paths[0]

            # Extraer las ciudades (nodos con 'name')
            nodos = [n["name"] for n in best_path[0].nodes if "name" in n]
            return {
                "ruta": nodos,
                "tiempo_total": best_path[1],
                "coste_total": best_path[2]
            }

    def _calcular_tiempo_coste_ruta(self, path):
        # Calcula tiempo y coste sumando cada RouteSegment en la ruta
        tiempo_total, coste_total = 0, 0
        prev_transporte = None
        # Nodos RouteSegment tienen distancia_km y transporte
        for node in path.nodes:
            if node.get("distancia_km") and node.get("transporte"):
                dist = node["distancia_km"]
                transporte = node["transporte"]
                params = TRANSPORT_PARAMS[transporte]

                # Cálculo de tiempo y coste por segmento
                tiempo_segmento = params["min_100km"] * (dist / 100)
                coste_segmento = params["coste_100km"] * (dist / 100)
                if prev_transporte and prev_transporte != transporte:
                    # Cambio de transporte, añadir tiempo carga/descarga
                    tiempo_segmento += params["carga_descarga"]

                tiempo_total += tiempo_segmento
                coste_total += coste_segmento
                prev_transporte = transporte

        return tiempo_total, coste_total

    def _cumple_restricciones(self, tipo_envio, tiempo_total):
        # Calcula el tiempo disponible según tipo de envío
        now = datetime.now()
        if tipo_envio == 1:
            # Antes de las 19h - 1h => 18h
            limite = now.replace(hour=19, minute=0, second=0, microsecond=0) - timedelta(hours=1)
            tiempo_disponible = (limite - now).total_seconds() / 60.0
            return tiempo_total <= tiempo_disponible
        elif tipo_envio == 2:
            # Al día siguiente antes de las 14h
            manana = now + timedelta(days=1)
            limite = manana.replace(hour=14, minute=0, second=0, microsecond=0)
            tiempo_disponible = (limite - now).total_seconds() / 60.0
            return tiempo_total <= tiempo_disponible
        elif tipo_envio == 3:
            # Sin límite
            return True

    def assign_vehicle_to_route(self, route_nodes, transporte):
        # Asigna vehículos a cada tramo (start->end) buscando un RouteSegment con transporte
        # (start)-[:SEGMENT]-(rs:RouteSegment{transporte})-[:SEGMENT]-(end)
        vehicles_assigned = []
        with self.driver.session() as session:
            for i in range(len(route_nodes) - 1):
                start = route_nodes[i]
                end = route_nodes[i+1]
                res = session.run("""
                    MATCH (start {name:$start})-[:SEGMENT]-(rs:RouteSegment {transporte:$transporte})-[:SEGMENT]-(end {name:$end})
                    OPTIONAL MATCH (v:Vehicle)-[:CUBRE]->(rs)
                    RETURN rs, v.unique_id AS vid LIMIT 1
                """, start=start, end=end, transporte=transporte)
                record = res.single()

                if record is None:
                    # No existe tramo con ese transporte entre start y end
                    raise ValueError("No existe el tramo solicitado con ese transporte.")

                vid = record["vid"]
                if vid is not None:
                    # Ya hay un vehículo asignado a este tramo
                    vehicles_assigned.append(vid)
                else:
                    # Crear un nuevo vehículo
                    unique_id = str(uuid.uuid4())
                    new_v = session.run("""
                        MATCH (start {name:$start})-[:SEGMENT]-(rs:RouteSegment {transporte:$transporte})-[:SEGMENT]-(end {name:$end})
                        CREATE (v:Vehicle {
                            unique_id: $unique_id,
                            transporte:$transporte,
                            last_node:$start,
                            timestamp:$ts
                        })-[:CUBRE]->(rs)
                        RETURN v.unique_id AS vid
                    """, start=start, end=end, transporte=transporte, ts=datetime.now().isoformat(), unique_id=unique_id).single()
                    vehicles_assigned.append(new_v["vid"])

        return vehicles_assigned

    def update_vehicle_position(self, vehicle_id, next_node):
        # Actualiza la posición del vehículo
        with self.driver.session() as session:
            session.run("""
                MATCH (v:Vehicle {unique_id: $vid})
                SET v.last_node = $last_node, v.timestamp = $ts
            """, vid=vehicle_id, last_node=next_node, ts=datetime.now().isoformat())

    def manage_package(self, compra_id, tipo_envio, ruta_info, vehicles_assigned):
        # Crea un paquete y lo asocia a los vehículos
        with self.driver.session() as session:
            package_res = session.run("""
                CREATE (p:Package {
                    compra_id:$compra_id,
                    tipo_envio:$tipo_envio,
                    tiempo_total:$tiempo_total,
                    coste_total:$coste_total,
                    ruta:$ruta,
                    created_at:$created_at
                }) RETURN p
            """, compra_id=str(compra_id), tipo_envio=tipo_envio,
               tiempo_total=ruta_info["tiempo_total"], coste_total=ruta_info["coste_total"],
               ruta="->".join(ruta_info["ruta"]), created_at=datetime.now().isoformat())
            p_record = package_res.single()
            p_id = p_record["p"].id

            for vid in vehicles_assigned:
                session.run("""
                    MATCH (p:Package), (v:Vehicle {unique_id:$vid})
                    WHERE id(p) = $pid
                    CREATE (p)-[:USADO_POR]->(v)
                """, pid=p_id, vid=vid)

            return p_id

    def get_package_status(self, package_id):
        # Devuelve el estado del paquete, incluyendo ubicacion_actual y tiempo_restante_aprox
        with self.driver.session() as session:
            pkg = session.run("""
                MATCH (p:Package) WHERE id(p) = $pid
                RETURN p.compra_id AS compra_id, p.ruta AS ruta, p.tiempo_total AS tiempo_total
            """, pid=package_id).single()

            if not pkg:
                return None

            ruta = pkg["ruta"].split("->")
            ubicacion_actual = ruta[0] if ruta else ""
            tiempo_restante_aprox = float(pkg["tiempo_total"]) / 2

            return {
                "compra_id": pkg["compra_id"],
                "ruta": pkg["ruta"],
                "tiempo_total": pkg["tiempo_total"],
                "ubicacion_actual": ubicacion_actual,
                "tiempo_restante_aprox": tiempo_restante_aprox
            }

if __name__ == "__main__":
    print("LogisticsManager listo y comentado.")
