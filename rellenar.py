from models import Cliente, Producto, Compra, Proveedor, Direccion, init_app
from datetime import datetime, timedelta
import random

def seed_data():
    # Inicializar la aplicación y los modelos
    init_app()

    # Crear varios proveedores con direcciones diferentes
    proveedores = []
    nombres_proveedores = ["Proveedor Central", "Distribuciones Norte", "Logística Sur", "Proveedor Este"]
    direcciones_proveedores = [
        {"calle": "Calle de Alcalá", "numero": "50", "ciudad": "Madrid", "codigo_postal": "28014", "pais": "España"},
        {"calle": "Carrer de la Pau", "numero": "12", "ciudad": "Valencia", "codigo_postal": "46002", "pais": "España"},
        {"calle": "Avenida Libertad", "numero": "5", "ciudad": "Sevilla", "codigo_postal": "41001", "pais": "España"},
        {"calle": "Rua Augusta", "numero": "100", "ciudad": "Lisboa", "codigo_postal": "1100-053", "pais": "Portugal"}
    ]

    for i, nombre in enumerate(nombres_proveedores):
        proveedor = Proveedor(
            nombre=nombre,
            direcciones_almacenes=[direcciones_proveedores[i]]
        )
        proveedor.save()
        proveedores.append(proveedor)
        print(f"Proveedor guardado: {proveedor.to_dict()}")

    # Crear varios productos con proveedores asignados
    productos = []
    for i in range(1, 21):
        producto = Producto(
            nombre=f"Producto {i}",
            codigo_producto_proveedor=f"PRD{i:03d}",
            precio=round(random.uniform(10, 200), 2),
            dimensiones={"ancho": random.randint(5, 50), "alto": random.randint(5, 50), "profundidad": random.randint(5, 50)},
            peso=round(random.uniform(0.5, 10), 2),
            proveedores=[random.choice(proveedores)]
        )
        producto.save()
        productos.append(producto)
        print(f"Producto guardado: {producto.to_dict()}")

    # Crear varios clientes con direcciones de envío
    clientes = []
    nombres_clientes = ["Luis Martínez", "Ana Sánchez", "Carlos López", "Beatriz Gómez", "María Fernández"]
    direcciones_clientes = [
        {"calle": "Calle Mayor", "numero": "15", "ciudad": "Madrid", "codigo_postal": "28013", "pais": "España"},
        {"calle": "Avenida de la Paz", "numero": "8", "ciudad": "Barcelona", "codigo_postal": "08002", "pais": "España"},
        {"calle": "Rúa Nova", "numero": "20", "ciudad": "A Coruña", "codigo_postal": "15003", "pais": "España"},
        {"calle": "Calle 10 de Agosto", "numero": "3", "ciudad": "Bilbao", "codigo_postal": "48001", "pais": "España"},
        {"calle": "Rua Bela Vista", "numero": "25", "ciudad": "Lisboa", "codigo_postal": "1100-300", "pais": "Portugal"}
    ]

    for i, nombre in enumerate(nombres_clientes):
        cliente = Cliente(
            nombre=nombre,
            fecha_alta=(datetime.now() - timedelta(days=random.randint(100, 1000))).strftime("%Y-%m-%d"),
            direcciones_envio=[direcciones_clientes[i]]
        )
        cliente.save()
        clientes.append(cliente)
        print(f"Cliente guardado: {cliente.to_dict()}")

    # Crear varias compras asociadas a clientes y productos
    for _ in range(30):
        cliente = random.choice(clientes)
        producto_seleccionado = random.sample(productos, k=random.randint(1, 5))
        precio_total = sum([p.precio for p in producto_seleccionado])
        direccion_envio = random.choice(cliente.direcciones_envio)
        
        compra = Compra(
            productos=producto_seleccionado,
            cliente=cliente,
            precio_compra=precio_total,
            fecha_compra=(datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d"),
            direccion_envio=direccion_envio
        )
        compra.save()
        print(f"Compra guardada: {compra.to_dict()}")

if __name__ == "__main__":
    seed_data()
