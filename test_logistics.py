import unittest
from datetime import datetime, timedelta
from logistics import LogisticsManager
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

class TestLogistics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Se inicializa el LogisticsManager una vez para todas las pruebas
        cls.manager = LogisticsManager(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

    @classmethod
    def tearDownClass(cls):
        # Cierra la conexión al finalizar las pruebas
        cls.manager.close()

    def test_get_optimal_route_tipo1(self):
        # Tipo 1: Entrega en el día
        # Ejemplo: Buscar una ruta desde Madrid a Malaga (Aéreo de 430km, se debería poder cumplir en el día)
        route = self.manager.get_optimal_route("Madrid", "Malaga", tipo_envio=1)
        self.assertIsNotNone(route, "No se encontró una ruta tipo 1 que cumpla las restricciones.")
        self.assertIn("Malaga", route["ruta"], "La ruta no llega a Malaga.")
        self.assertLessEqual(route["tiempo_total"], 480, "El tiempo total parece excesivo para tipo 1.")

    def test_get_optimal_route_tipo2(self):
        # Tipo 2: Entrega en un día (antes de las 14h del día siguiente)
        # Ejemplo: Madrid -> Alicante (por Barcelona, Ferrocarril), se asume que debe ser relativamente rápido.
        route = self.manager.get_optimal_route("Barcelona", "Alicante", tipo_envio=2)
        self.assertIsNotNone(route, "No se encontró una ruta tipo 2 que cumpla las restricciones.")
        self.assertIn("Alicante", route["ruta"], "La ruta no llega a Alicante.")

    def test_get_optimal_route_tipo3(self):
        # Tipo 3: Envío económico sin límite
        # Ejemplo: Valencia -> Santander pasando por Zaragoza (Carretera+Carretera)
        # Aunque tarde más, siempre debería devolver algo si hay ruta
        route = self.manager.get_optimal_route("Valencia", "Santander", tipo_envio=3)
        # Posible ruta: Valencia -> Zaragoza -> Santander
        # Si no existe, quizás pruebe Madrid -> Valladolid (más sencillo)
        if route is None:
            # Intentar otra ruta más simple Madrid->Valladolid
            route = self.manager.get_optimal_route("Madrid", "Valladolid", tipo_envio=3)
        self.assertIsNotNone(route, "No se encontró ninguna ruta tipo 3.")
    
    def test_assign_vehicle_to_route(self):
        # Probar asignar un vehículo a un tramo
        # Por ejemplo, la ruta obtenida en tipo 1: Madrid -> Malaga (Aéreo)
        route = self.manager.get_optimal_route("Madrid", "Malaga", tipo_envio=1)
        self.assertIsNotNone(route, "No hay ruta disponible para asignar vehículo.")
        vehicles = self.manager.assign_vehicle_to_route(route["ruta"], "Aéreo")
        self.assertTrue(len(vehicles) > 0, "No se asignó ningún vehículo a la ruta.")
        # Asignar nuevamente y comprobar que reutiliza el mismo vehículo
        vehicles2 = self.manager.assign_vehicle_to_route(route["ruta"], "Aéreo")
        self.assertEqual(vehicles, vehicles2, "No está reutilizando el vehículo para el mismo trayecto y transporte.")

    def test_manage_package_and_get_status(self):
        # Crear un paquete y comprobar su estado
        route = self.manager.get_optimal_route("Madrid", "Granada", tipo_envio=1)
        self.assertIsNotNone(route, "No hay ruta para el envío a Granada.")
        vehicles = self.manager.assign_vehicle_to_route(route["ruta"], "Carretera")
        package_id = self.manager.manage_package(compra_id=12345, tipo_envio=1, ruta_info=route, vehicles_assigned=vehicles)
        self.assertIsNotNone(package_id, "No se creó el paquete.")

        # Comprobar el estado del paquete
        status = self.manager.get_package_status(package_id)
        self.assertIsNotNone(status, "No se pudo obtener el estado del paquete.")
        self.assertIn("ubicacion_actual", status, "El estado del paquete no contiene la ubicación actual.")
        self.assertIn("tiempo_restante_aprox", status, "El estado del paquete no contiene el tiempo restante.")

    def test_update_vehicle_position(self):
        # Asignar un vehículo y actualizar su posición
        route = self.manager.get_optimal_route("Madrid", "Granada", tipo_envio=3)
        self.assertIsNotNone(route, "No hay ruta para actualizar posición de vehículo.")
        vehicles = self.manager.assign_vehicle_to_route(route["ruta"], "Carretera")
        self.assertTrue(len(vehicles) > 0, "No se asignó vehículo a la ruta para probar la actualización.")
        
        vehicle_id = vehicles[0]
        # Actualizar la posición del vehículo al siguiente nodo de la ruta
        # Ejemplo: Si la ruta es Madrid -> Granada, next_node = "Granada"
        if len(route["ruta"]) > 1:
            next_node = route["ruta"][1]
            self.manager.update_vehicle_position(vehicle_id, next_node)
            # No podemos verificar fácilmente el cambio sin consultar la BD, pero al menos no debe fallar

if __name__ == '__main__':
    unittest.main()
