# logistics.py
import os
from datetime import datetime, timedelta
from neo4j import GraphDatabase

# Parámetros de transporte y sus costes/tiempos asociados
TRANSPORT_PARAMS = {
    "Carretera": {"min_100km": 60, "carga_descarga": 5, "coste_100km": 1.0},
    "Ferrocarril": {"min_100km": 50, "carga_descarga": 10, "coste_100km": 0.8},
    "Aéreo": {"min_100km": 10, "carga_descarga": 40, "coste_100km": 3.5},
    "Marítimo": {"min_100km": 120, "carga_descarga": 20, "coste_100km": 0.3},
}

class LogisticsManager:
    def __init__(self, uri, user, password):
        # Conexión con Neo4j
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Cierra la conexión
        self.driver.close()

    def get_optimal_route(self, start_name, end_name, tipo_envio):
        """
        Obtiene la ruta óptima (más económica dentro de las restricciones de tiempo) entre
        start_name (almacén) y end_name (punto de entrega), teniendo en cuenta el tipo de envío.
        
        - tipo_envio 1: entrega en el mismo día (antes de las 19h menos 1h para empaquetar)
        - tipo_envio 2: entrega al día siguiente antes de las 14h
        - tipo_envio 3: envío económico (sin límite de tiempo)
        
        Retorna un diccionario con 'ruta', 'tiempo_total' y 'coste_total' si hay ruta válida,
        de lo contrario None.
        """
        with self.driver.session() as session:
            # Se obtiene el shortestPath en Neo4j para limitar el número de rutas a evaluar
            result = session.run("""
                MATCH (start {name:$start_name}), (end {name:$end_name}),
                      p = shortestPath((start)-[:RUTA*..10]->(end))
                RETURN p
            """, start_name=start_name, end_name=end_name)

            candidate_paths = []
            # Se evalúan las rutas encontradas
            for record in result:
                p = record["p"]
                tiempo_total, coste_total = self._calcular_tiempo_coste_ruta(p)
                # Verificar restricciones de tiempo según el tipo de envío
                if self._cumple_restricciones(tipo_envio, tiempo_total):
                    candidate_paths.append((p, tiempo_total, coste_total))

            # Si no hay rutas que cumplan restricciones, retornar None
            if not candidate_paths:
                return None

            # Seleccionar la ruta más económica entre las que cumplen la restricción
            candidate_paths.sort(key=lambda x: x[2])  # Ordenar por coste_total
            best_path = candidate_paths[0]
            nodos = [n["name"] for n in best_path[0].nodes]

            return {
                "ruta": nodos,
                "tiempo_total": best_path[1],
                "coste_total": best_path[2]
            }

    def _calcular_tiempo_coste_ruta(self, path):
        """
        Dado un path de Neo4j, calcula el tiempo y el coste total considerando:
        - Distancia por tramo.
        - Tipo de transporte (min/100km, coste/100km).
        - Tiempo de carga/descarga si cambia el transporte entre tramos.
        """
        relaciones = path.relationships
        tiempo_total, coste_total = 0, 0
        prev_transporte = None

        for rel in relaciones:
            dist = rel["distancia_km"]
            transporte = rel["transporte"]
            params = TRANSPORT_PARAMS[transporte]

            # Calcular el tiempo y el coste en base a la distancia
            tiempo_segmento = params["min_100km"] * (dist / 100)
            coste_segmento = params["coste_100km"] * (dist / 100)

            # Si cambia el transporte, añadir el tiempo de carga/descarga
            if prev_transporte and prev_transporte != transporte:
                tiempo_segmento += params["carga_descarga"]

            tiempo_total += tiempo_segmento
            coste_total += coste_segmento
            prev_transporte = transporte

        return tiempo_total, coste_total

    def _cumple_restricciones(self, tipo_envio, tiempo_total):
        """
        Verifica si el tiempo_total cumple con las restricciones del tipo de envío.
        Se asume que el envío se realiza 'ahora' (datetime.now()).
        """
        now = datetime.now()
        if tipo_envio == 1:
            # Entrega antes de las 19h - 1h empaquetado => límite 18h
            limite = now.replace(hour=19, minute=0, second=0, microsecond=0) - timedelta(hours=1)
            tiempo_disponible = (limite - now).total_seconds() / 60.0
            return tiempo_total <= tiempo_disponible
        elif tipo_envio == 2:
            # Entrega al día siguiente antes de las 14h
            manana = now + timedelta(days=1)
            limite = manana.replace(hour=14, minute=0, second=0, microsecond=0)
            tiempo_disponible = (limite - now).total_seconds() / 60.0
            return tiempo_total <= tiempo_disponible
        elif tipo_envio == 3:
            # Sin límite de tiempo
            return True

    def assign_vehicle_to_route(self, route_nodes, transporte):
        """
        Asigna vehículos a los tramos de una ruta.
        Si ya existe un vehículo para un trayecto (start->end) y un tipo de transporte dado,
        lo reutiliza. De lo contrario, crea uno nuevo.
        
        Retorna una lista de IDs de los vehículos asignados.
        """
        vehicles_assigned = []
        with self.driver.session() as session:
            for i in range(len(route_nodes) - 1):
                start, end = route_nodes[i], route_nodes[i+1]
                # Verificar si existe un vehículo para este trayecto y transporte
                res = session.run("""
                    MATCH (s {name:$start})-[r:RUTA {transporte:$transporte}]->(e {name:$end})
                    OPTIONAL MATCH (v:Vehicle)-[:CUBRE]->(r)
                    RETURN id(v) AS vid LIMIT 1
                """, start=start, end=end, transporte=transporte)
                record = res.single()

                if record and record["vid"] is not None:
                    # Existe un vehículo, reutilizarlo
                    vid = record["vid"]
                    vehicles_assigned.append(vid)
                else:
                    # No existe, crear uno nuevo
                    new_v = session.run("""
                        MATCH (s {name:$start})-[r:RUTA {transporte:$transporte}]->(e {name:$end})
                        CREATE (v:Vehicle {
                            transporte:$transporte,
                            last_node:$start,
                            timestamp:$ts
                        })-[:CUBRE]->(r)
                        RETURN id(v) AS vid
                    """, start=start, end=end, transporte=transporte, ts=datetime.now().isoformat()).single()
                    vehicles_assigned.append(new_v["vid"])

        return vehicles_assigned

    def update_vehicle_position(self, vehicle_id, next_node):
        """
        Actualiza la posición del vehículo al pasar por next_node.
        Esto permite registrar el último nodo visitado y el timestamp,
        cumpliendo con el requisito de actualizar la situación del vehículo.
        """
        with self.driver.session() as session:
            session.run("""
                MATCH (v:Vehicle)
                WHERE id(v) = $vid
                SET v.last_node = $last_node, v.timestamp = $ts
            """, vid=vehicle_id, last_node=next_node, ts=datetime.now().isoformat())

    def manage_package(self, compra_id, tipo_envio, ruta_info, vehicles_assigned):
        """
        Registra un paquete (envío) en Neo4j, guardando:
        - tipo de servicio
        - tiempo total
        - coste total
        - la ruta elegida
        - asigna los vehículos utilizados al paquete

        Devuelve el ID del paquete creado.
        """
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

            # Asociar vehículos al paquete
            for vid in vehicles_assigned:
                session.run("""
                    MATCH (p:Package), (v:Vehicle)
                    WHERE id(p) = $pid AND id(v) = $vid
                    CREATE (p)-[:USADO_POR]->(v)
                """, pid=p_id, vid=vid)

            return p_id

    def get_package_status(self, package_id):
        """
        Obtiene la ubicación actual del paquete y el tiempo restante aproximado.
        Esto se logra consultando:
        - La ruta del paquete.
        - La posición del vehículo (last_node, timestamp).
        - Calculando cuánto falta para llegar al destino asumiendo
          que el vehículo continúa por la ruta establecida.
        """
        with self.driver.session() as session:
            pkg = session.run("""
                MATCH (p:Package) WHERE id(p) = $pid
                RETURN p.compra_id AS compra_id, p.ruta AS ruta, p.tiempo_total AS tiempo_total
            """, pid=package_id).single()

            if not pkg:
                return None

            ruta_str = pkg["ruta"]
            nodos_ruta = ruta_str.split("->")

            vehicles = session.run("""
                MATCH (p:Package)-[:USADO_POR]->(v:Vehicle)
                WHERE id(p) = $pid
                RETURN id(v) AS vid, v.transporte AS transporte, v.last_node AS last_node, v.timestamp AS ts
            """, pid=package_id).values()

            # Si no hay vehículos, se asume que el paquete está en el origen
            if not vehicles:
                return {
                    "compra_id": pkg["compra_id"],
                    "ubicacion_actual": nodos_ruta[0],
                    "tiempo_restante_aprox": pkg["tiempo_total"]
                }

            # Tomar el primer vehículo (asumiendo uno principal)
            vid, transporte, last_node, ts = vehicles[0]

            # Reconstruir los tramos de la ruta para calcular tiempo restante
            relations = session.run("""
                UNWIND $r AS nod
                WITH $r AS r
                UNWIND RANGE(0, SIZE(r)-2) AS i
                MATCH (a {name:r[i]})-[rel:RUTA]->(b {name:r[i+1]})
                RETURN rel, a.name AS start, b.name AS end
            """, r=nodos_ruta)

            tramos = relations.values()
            tiempo_restante = 0
            prev_transporte = None
            calcular = False

            for val in tramos:
                rel = val[0]
                start = val[1]
                end = val[2]
                dist = rel["distancia_km"]
                t = rel["transporte"]
                params = TRANSPORT_PARAMS[t]

                tiempo_segmento = params["min_100km"] * (dist / 100)
                if prev_transporte and prev_transporte != t:
                    tiempo_segmento += params["carga_descarga"]
                prev_transporte = t

                # Cuando encontramos el last_node del vehiculo, comenzamos a calcular el tiempo restante
                if start == last_node:
                    calcular = True
                    tiempo_restante += tiempo_segmento
                elif calcular:
                    tiempo_restante += tiempo_segmento

            # Si no se ha encontrado last_node, asumimos que el vehículo no se ha movido
            if not calcular:
                tiempo_restante = pkg["tiempo_total"]

            return {
                "compra_id": pkg["compra_id"],
                "ubicacion_actual": last_node,
                "tiempo_restante_aprox": tiempo_restante
            }

if __name__ == "__main__":
    # Mensaje informativo si se ejecuta este archivo directamente
    print("LogisticsManager se usa para gestionar rutas, vehículos y paquetes.")
    print("Use initialize_db.py para inicializar la infraestructura.")
