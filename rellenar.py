from models import Cliente, Producto, Compra, Proveedor, Direccion, init_app
from datetime import datetime, timedelta
import random

def seed_data():
    # Inicializar la aplicación y los modelos
    init_app()

    # Crear proveedores, incluyendo "Modas Paqui"
    proveedores = []
    nombres_proveedores = [
        "Proveedor Central",
        "Distribuciones Norte",
        "Logística Sur",
        "Proveedor Este",
        "Modas Paqui"  # Añadimos "Modas Paqui"
    ]
    direcciones_proveedores = [
        {"calle": "Calle de Alcalá", "numero": "50", "ciudad": "Madrid", "codigo_postal": "28014", "pais": "España"},
        {"calle": "Carrer de la Pau", "numero": "12", "ciudad": "Valencia", "codigo_postal": "46002", "pais": "España"},
        {"calle": "Avenida de Andalucía", "numero": "5", "ciudad": "Sevilla", "codigo_postal": "41001", "pais": "España"},
        {"calle": "Rua Augusta", "numero": "100", "ciudad": "Lisboa", "codigo_postal": "1100-053", "pais": "Portugal"},
        {"calle": "Calle de la Moda", "numero": "123", "ciudad": "Madrid", "codigo_postal": "28015", "pais": "España"}  # Dirección de "Modas Paqui"
    ]

    for i, nombre in enumerate(nombres_proveedores):
        direccion_almacen = Direccion(**direcciones_proveedores[i])
        # Forzar la ubicación manualmente si es necesario (ejemplo con coordenadas conocidas)
        if nombre == "Modas Paqui":
            direccion_almacen.location = {'type': 'Point', 'coordinates': [-3.703790, 40.416775]}  # Coordenadas de Madrid
        # No llamar a direccion_almacen.save()

        proveedor = Proveedor(
            nombre=nombre,
            direcciones_almacenes=[direccion_almacen]
        )
        proveedor.save()
        proveedores.append(proveedor)
        print(f"Proveedor guardado: {proveedor.to_dict()}")

    # Crear productos, incluyendo algunos con "manga corta" en el nombre
    productos = []
    nombres_productos = [
        "Camiseta de manga corta blanca",
        "Pantalón vaquero",
        "Vestido de fiesta",
        "Camisa de manga larga",
        "Camiseta de manga corta negra",
        "Zapatos deportivos",
        "Chaqueta de cuero",
        "Camiseta de manga corta roja",
        "Jersey de lana",
        "Falda plisada",
        "Camiseta de manga corta azul",
        "Sombrero de paja",
        "Polo de manga corta verde",
        "Traje de baño",
        "Calcetines de algodón",
        "Bufanda de seda",
        "Gafas de sol",
        "Cinturón de cuero",
        "Guantes de invierno",
        "Zapatos de vestir"
    ]

    for i, nombre_producto in enumerate(nombres_productos):
        # Asignar "Modas Paqui" como proveedor para productos con "manga corta" en el nombre
        if "manga corta" in nombre_producto.lower():
            proveedores_producto = [p for p in proveedores if p.nombre == "Modas Paqui"]
        else:
            proveedores_producto = [random.choice(proveedores)]
        producto = Producto(
            nombre=nombre_producto,
            codigo_producto_proveedor=f"PRD{i+1:03d}",
            precio=round(random.uniform(10, 200), 2),
            dimensiones={"ancho": random.randint(5, 50), "alto": random.randint(5, 50), "profundidad": random.randint(5, 50)},
            peso=round(random.uniform(0.5, 10), 2),
            proveedores=proveedores_producto
        )
        producto.save()
        productos.append(producto)
        print(f"Producto guardado: {producto.to_dict()}")

    # Crear clientes, incluyendo "Beatriz Gómez"
    clientes = []
    nombres_clientes = ["Luis Martínez", "Ana Sánchez", "Carlos López", "Beatriz Gómez", "María Fernández"]
    direcciones_base = [
        {"calle": "Calle Mayor", "ciudad": "Madrid", "codigo_postal": "28013", "pais": "España"},
        {"calle": "Passeig de Gràcia", "ciudad": "Barcelona", "codigo_postal": "08007", "pais": "España"},
        {"calle": "Gran Vía", "ciudad": "Madrid", "codigo_postal": "28013", "pais": "España"},
        {"calle": "Avenida de la Constitución", "ciudad": "Sevilla", "codigo_postal": "41001", "pais": "España"},
        {"calle": "Praça do Comércio", "ciudad": "Lisboa", "codigo_postal": "1100-148", "pais": "Portugal"}
    ]

    for i, nombre in enumerate(nombres_clientes):
        # Crear múltiples direcciones de envío para cada cliente
        direcciones_envio = []
        num_direcciones = random.randint(2, 4)  # Cada cliente tendrá entre 2 y 4 direcciones de envío
        for j in range(num_direcciones):
            direccion_data = direcciones_base[i].copy()
            direccion_data["numero"] = str(random.randint(1, 100))
            direccion_envio = Direccion(**direccion_data)
            # Forzar la ubicación manualmente si es necesario
            if nombre == "Beatriz Gómez" and j == 0:
                direccion_envio.location = {'type': 'Point', 'coordinates': [-5.996295, 37.389092]}  # Coordenadas de Sevilla
            # No llamar a direccion_envio.save()
            direcciones_envio.append(direccion_envio)

        cliente = Cliente(
            nombre=nombre,
            fecha_alta=(datetime.now() - timedelta(days=random.randint(100, 1000))).strftime("%Y-%m-%d"),
            direcciones_envio=direcciones_envio
        )
        cliente.save()
        clientes.append(cliente)
        print(f"Cliente guardado: {cliente.to_dict()}")

    # Crear compras, asegurando que "Beatriz Gómez" tiene compras en "2024-04-11"
    for _ in range(25):
        cliente = random.choice(clientes)
        producto_seleccionado = random.sample(productos, k=random.randint(1, 5))
        precio_total = sum([p.precio for p in producto_seleccionado])
        fecha_compra = (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d")
        # Seleccionar una dirección de envío aleatoria del cliente
        direccion_envio = random.choice(cliente.direcciones_envio)

        compra = Compra(
            productos=producto_seleccionado,
            cliente=cliente,
            precio_compra=precio_total,
            fecha_compra=fecha_compra,
            direccion_envio=direccion_envio
        )
        compra.save()
        print(f"Compra guardada: {compra.to_dict()}")

    # Añadir compras específicas para "Beatriz Gómez" el "2024-04-11"
    fecha_especifica = datetime(2024, 4, 11).strftime("%Y-%m-%d")
    cliente_beatriz = next((c for c in clientes if c.nombre == "Beatriz Gómez"), None)
    if cliente_beatriz:
        for _ in range(5):  # Crear 5 compras en esa fecha
            producto_seleccionado = random.sample(productos, k=random.randint(1, 5))
            precio_total = sum([p.precio for p in producto_seleccionado])
            # Seleccionar una dirección de envío aleatoria de Beatriz Gómez
            direccion_envio = random.choice(cliente_beatriz.direcciones_envio)

            compra = Compra(
                productos=producto_seleccionado,
                cliente=cliente_beatriz,
                precio_compra=precio_total,
                fecha_compra=fecha_especifica,
                direccion_envio=direccion_envio
            )
            compra.save()
            print(f"Compra guardada: {compra.to_dict()}")

if __name__ == "__main__":
    seed_data()
