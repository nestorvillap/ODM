# **MongoDB ODM for Embedded and Referenced Documents**

A powerful Object-Document Mapper (ODM) for MongoDB, designed for complex schemas with embedded and referenced documents, geolocation support, and flexible data validation.

## **Features**

- **Nested Document Support**: Handles embedded and referenced documents, perfect for complex data models.
- **Geolocation Integration**: Supports GeoJSON fields and geopy-based geolocation, ideal for spatial queries.
- **Flexible Schema Definition**: Define required and admissible fields for each model.
- **Aggregation Pipelines**: Includes support for advanced MongoDB aggregation pipelines.

## **Getting Started**

1. **Install Dependencies**:
   ```bash
   pip install pymongo geopy geojson
   ```

2. **Configure MongoDB**: Set up `URL_DB` and `DB_NAME` in your configuration file.

3. **Define Models**: Create models by subclassing `Model` and specifying required and admissible fields.

4. **CRUD Operations**: Use built-in methods (`save`, `delete`, `find`) for document management.

5. **Aggregation**: Use the `aggregate` method to run complex queries and pipelines.

## **Example Usage**

```python
from models import Cliente, Producto, Compra, init_app
from datetime import datetime

# Initialize models
init_app()

# Create a new client
cliente = Cliente(nombre="Juan Perez", fecha_alta=datetime.now())
cliente.save()

# Query clients
for cliente in Cliente.find({"nombre": "Juan Perez"}):
    print(cliente.to_dict())
```

## **Requirements**

- Python 3.7+
- MongoDB
- `pymongo`, `geopy`, `geojson`

## **Contributing**

Contributions are welcome! Please open an issue or submit a pull request for improvements.
