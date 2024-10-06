from models import Cliente, Producto, Compra, Proveedor, initApp
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time
from geojson import Point

# Función para obtener las coordenadas a partir de una dirección
def getLocationPoint(address: str) -> Point:
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
    location = None
    while location is None:
        try:
            time.sleep(1)
            location = Nominatim(user_agent="Mi-Nombre-Aleatorio").geocode(address)
        except GeocoderTimedOut:
            continue
    return Point((location.longitude, location.latitude))

# Inicializar la aplicación y realizar operaciones CRUD
def main():
    # Inicializar base de datos y modelos
    initApp()

    # Crear un cliente de ejemplo
    # Hacer pruebas para comprobar que funciona correctamente el modelo
    cliente = Cliente(nombre="Juan Perez", fecha_alta="10/10/2021", direcciones=[
        {"calle": "Calle 123", "ciudad": "Madrid", "codigo_postal": "28001"},
        {"calle": "Calle 456", "ciudad": "Barcelona", "codigo_postal": "08001"}
    ])
    cliente.save()
    print(f"Cliente guardado: {cliente.__dict__}")

    # Asignar nuevo valor a variable admitida del objeto
    cliente.nombre = "Juan Actualizado"
    print(f"Nombre actualizado del cliente antes de guardar: {cliente.nombre}")

    # Asignar nuevo valor a variable no admitida del objeto
    try:
        cliente.nota = "Cliente preferencial"
    except AttributeError as e:
        print(f"Error al asignar una variable no admitida: {e}")

    # Guardar el cliente actualizado
    cliente.save()
    print(f"Cliente actualizado: {cliente.__dict__}")

    # Asignar nuevo valor a variable admitida del objeto
    cliente.fecha_alta = "11/10/2021"
    print(f"Fecha de alta actualizada del cliente antes de guardar: {cliente.fecha_alta}")

    # Guardar el cliente con el nuevo valor
    cliente.save()
    print(f"Cliente actualizado nuevamente: {cliente.__dict__}")

    # Buscar el cliente por un filtro
    clientes = Cliente.find({"nombre": "Juan Actualizado"})
    for c in clientes:
        print(f"Cliente encontrado: {c.__dict__}")

    # Obtener primer documento
    primer_cliente = Cliente.find({}).__iter__().__next__()
    print(f"Primer cliente encontrado: {primer_cliente.__dict__}")

    # Modificar valor de variable admitida
    primer_cliente.nombre = "Juan Modificado"
    primer_cliente.save()
    print(f"Primer cliente modificado: {primer_cliente.__dict__}")

    # Crear un producto de ejemplo
    producto = Producto(nombre="Laptop", codigo_producto_proveedor="LPT123", precio=1200.0, dimensiones="30x20x3 cm", peso="1.5 kg", proveedores=["Proveedor1", "Proveedor2"])
    producto.save()
    print(f"Producto guardado: {producto.__dict__}")

    # Crear una compra de ejemplo
    compra = Compra(productos=["Laptop"], cliente=cliente._id, precio_compra=1200.0, fecha_compra="15/10/2021", direccion_envio="Calle 123, Madrid")
    compra.save()
    print(f"Compra guardada: {compra.__dict__}")

    # Crear un proveedor de ejemplo
    proveedor = Proveedor(nombre="Proveedor1", direcciones_almacenes=["Almacen1", "Almacen2"])
    proveedor.save()
    print(f"Proveedor guardado: {proveedor.__dict__}")

if __name__ == "__main__":
    main()