from models import Cliente, Producto, Compra, Proveedor, Direccion, init_app

def main():
    # Inicializar la aplicación y los modelos
    init_app()

    # Crear un proveedor de ejemplo
    proveedor = Proveedor(
        nombre="Proveedor Ejemplo",
        direcciones_almacenes=[
            {
                "calle": "Calle de Alcalá",
                "numero": "50",
                "ciudad": "Madrid",
                "codigo_postal": "28014",
                "pais": "España"
            }
        ]
    )
    proveedor.save()
    print(f"Proveedor guardado: {proveedor.to_dict()}")

    # Crear un producto de ejemplo
    producto = Producto(
        nombre="Producto Ejemplo",
        codigo_producto_proveedor="ABC123",
        precio=99.99,
        dimensiones={"ancho": 10, "alto": 20, "profundidad": 5},
        peso=1.5,
        proveedores=[proveedor]
    )
    producto.save()
    print(f"Producto guardado: {producto.to_dict()}")

    # Crear un cliente de ejemplo
    cliente = Cliente(
        nombre="Pepe García",
        fecha_alta="05/11/2021",
        direcciones_envio=[
            {
                "calle": "Calle Gran Vía",
                "numero": "30",
                "ciudad": "Madrid",
                "codigo_postal": "28013",
                "pais": "España"
            }
        ]
    )
    cliente.save()
    print(f"Cliente guardado: {cliente.to_dict()}")

    # Crear una compra de ejemplo
    compra = Compra(
        productos=[producto],
        cliente=cliente,
        precio_compra=99.99,
        fecha_compra="06/11/2021",
        direccion_envio={
            "calle": "Avenida de la Constitución",
            "numero": "1",
            "ciudad": "Sevilla",
            "codigo_postal": "41001",
            "pais": "España"
        }
    )
    compra.save()
    print(f"Compra guardada: {compra.to_dict()}")

    # Modificar un atributo admitido del producto
    producto.precio = 89.99
    producto.save()
    print(f"Producto actualizado: {producto.to_dict()}")

    # Intentar asignar un atributo no admitido al producto
    try:
        producto.color = "Rojo"
    except AttributeError as e:
        print(f"Error: {e}")

    # Buscar productos por precio actualizado
    productos = Producto.find({"precio": 89.99})
    for p in productos:
        print(f"Producto encontrado: {p.to_dict()}")

    # Modificar el nombre del cliente
    cliente.nombre = "Nestor Villa"
    cliente.save()
    print(f"Cliente actualizado: {cliente.to_dict()}")

    # Buscar cliente por nombre actualizado
    clientes = Cliente.find({"nombre": "Nestor Villa"})
    for c in clientes:
        print(f"Cliente encontrado: {c.to_dict()}")

if __name__ == "__main__":
    main()
