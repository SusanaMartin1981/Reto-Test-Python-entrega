import psycopg2

# Conexión a la base de datos
try:
    connection = psycopg2.connect(
        dbname="Northwind",      # Nombre de tu base de datos
        user="postgres",         # Usuario de PostgreSQL
        password="Susana", # Contraseña de PostgreSQL
        host="localhost",        # Host donde está el servidor
        port="5432"              # Puerto del servidor
    )
    print("¡Conexión exitosa!")

# Ejercicio 2
# ¿Cuántos empleados tenemos contratados en 'Global Importaciones'? Indica su id, nombre, apellido, ciudad y país.

    # Consulta SQL
    #Crear un Cursor
    cursor=connection.cursor()
    # Consulta SQL sin filtros adicionales
    consulta = """
    SELECT 
        employee_id AS ID,
        first_name AS Nombre,
        last_name AS Apellido,
        city AS Ciudad,
        country AS País
    FROM 
        employees;
    """

    # Ejecutar la consulta
    cursor.execute(consulta)

    # Obtener e imprimir los resultados
    resultados = cursor.fetchall()
    for fila in resultados:
        print(f"ID: {fila[0]}, Nombre: {fila[1]}, Apellido: {fila[2]}, Ciudad: {fila[3]}, País: {fila[4]}")

    # Cerrar el cursor y la conexión
    cursor.close()
except Exception as e:
    print("Error al conectar:", e)
finally:
    if 'connection' in locals() and connection:
        connection.close()


#EJERCICIO
# ¿Qué productos tenemos? Indica el id del producto, id del proveedor, nombre del producto, precio por unidad, unidades en stock, unidades pedidas al proveedor y productos descontinuados.
import pandas as pd
import psycopg2

# Conexión a la base de datos PostgreSQL
conexion = psycopg2.connect(
    dbname="Northwind",      # Nombre de tu base de datos
    user="postgres",         # Usuario de PostgreSQL
    password="Susana",       # Contraseña de PostgreSQL
    host="localhost",        # Host donde está el servidor
    port="5432"              # Puerto del servidor
)

# Consulta SQL para obtener los datos de los productos
consulta = """
SELECT 
    product_id, 
    supplier_id, 
    product_name, 
    unit_price, 
    units_in_stock, 
    units_on_order, 
    discontinued
FROM 
    products;
"""

# Crear un DataFrame a partir de la consulta
productos_df = pd.read_sql_query(consulta, conexion)

# Cerrar la conexión
conexion.close()

# Mostrar los primeros registros del DataFrame
print(productos_df.head())

#EJERCICIO
import pandas as pd
import psycopg2

# Conexión a la base de datos PostgreSQL
conexion = psycopg2.connect(
    dbname="Northwind",      # Nombre de tu base de datos
    user="postgres",         # Usuario de PostgreSQL
    password="Susana",       # Contraseña de PostgreSQL
    host="localhost",        # Host donde está el servidor
    port="5432"              # Puerto del servidor
)

# Consulta SQL para productos descontinuados
consulta = """
SELECT 
    product_name, 
    units_in_stock
FROM 
    products
WHERE 
    discontinued = 1;
"""

# Crear un DataFrame con los resultados de la consulta
productos_descontinuados_df = pd.read_sql_query(consulta, conexion)

# Cerrar la conexión
conexion.close()

# Mostrar los primeros registros del DataFrame
print(productos_descontinuados_df.head())

#EJERCICIO
import pandas as pd
import psycopg2

# Conexión a la base de datos PostgreSQL
conexion = psycopg2.connect(
    dbname="Northwind",      # Nombre de tu base de datos
    user="postgres",         # Usuario de PostgreSQL
    password="Susana",       # Contraseña de PostgreSQL
    host="localhost",        # Host donde está el servidor
    port="5432"              # Puerto del servidor
)

# Consulta SQL para obtener datos sobre los proveedores
consulta = """
SELECT 
    supplier_id, 
    company_name, 
    city, 
    country
FROM 
    suppliers;
"""

# Crear un DataFrame con los resultados de la consulta
proveedores_df = pd.read_sql_query(consulta, conexion)

# Cerrar la conexión
conexion.close()

# Mostrar los primeros registros del DataFrame
print(proveedores_df.head())


#EJERCICIO
import pandas as pd
import psycopg2

# Conexión a la base de datos PostgreSQL
conexion = psycopg2.connect(
    dbname="Northwind",      # Nombre de tu base de datos
    user="postgres",         # Usuario de PostgreSQL
    password="Susana",       # Contraseña de PostgreSQL
    host="localhost",        # Host donde está el servidor
    port="5432"              # Puerto del servidor
)

# Consulta SQL para obtener los datos de los pedidos
consulta = """
SELECT 
    order_id, 
    customer_id, 
    ship_via AS shipper_id, 
    order_date, 
    required_date, 
    shipped_date
FROM 
    orders;
"""

# Crear un DataFrame con los resultados de la consulta
pedidos_df = pd.read_sql_query(consulta, conexion)

# Cerrar la conexión
conexion.close()

# Mostrar los primeros registros del DataFrame
print(pedidos_df.head())

#EJERCICIO
import pandas as pd
import psycopg2

# Conexión a la base de datos PostgreSQL
conexion = psycopg2.connect(
    dbname="Northwind",      # Nombre de tu base de datos
    user="postgres",         # Usuario de PostgreSQL
    password="Susana",       # Contraseña de PostgreSQL
    host="localhost",        # Host donde está el servidor
    port="5432"              # Puerto del servidor
)

# Consulta SQL para contar el número total de pedidos
consulta = """
SELECT 
    COUNT(*) AS total_pedidos
FROM 
    orders;
"""

# Ejecutar la consulta y guardar el resultado en un DataFrame
resultado_df = pd.read_sql_query(consulta, conexion)

# Cerrar la conexión
conexion.close()

# Obtener el número total de pedidos
total_pedidos = resultado_df['total_pedidos'][0]
print(f"El número total de pedidos registrados es: {total_pedidos}")


#EJERCICIO
import pandas as pd
import psycopg2

# Conexión a la base de datos PostgreSQL
conexion = psycopg2.connect(
    dbname="Northwind",      # Nombre de tu base de datos
    user="postgres",         # Usuario de PostgreSQL
    password="Susana",       # Contraseña de PostgreSQL
    host="localhost",        # Host donde está el servidor
    port="5432"              # Puerto del servidor
)

# Consulta SQL para obtener la información de los clientes
consulta = """
SELECT 
    customer_id, 
    company_name, 
    city, 
    country
FROM 
    customers;
"""

# Crear un DataFrame con los resultados de la consulta
clientes_df = pd.read_sql_query(consulta, conexion)

# Cerrar la conexión
conexion.close()

# Mostrar los primeros registros del DataFrame
print(clientes_df.head())

#EJERCICIO
import pandas as pd
import psycopg2

# Conexión a la base de datos PostgreSQL
conexion = psycopg2.connect(
    dbname="Northwind",      # Nombre de tu base de datos
    user="postgres",         # Usuario de PostgreSQL
    password="Susana",       # Contraseña de PostgreSQL
    host="localhost",        # Host donde está el servidor
    port="5432"              # Puerto del servidor
)

# Consulta SQL para obtener relaciones de reporte entre empleados y supervisores
consulta = """
SELECT 
    e.employee_id AS empleado_id, 
    e.first_name || ' ' || e.last_name AS empleado,
    e.reports_to AS id_supervisor,
    s.first_name || ' ' || s.last_name AS supervisor
FROM 
    employees e
LEFT JOIN 
    employees s 
ON 
    e.reports_to = s.employee_id;
"""

# Crear un DataFrame con los resultados de la consulta
relaciones_df = pd.read_sql_query(consulta, conexion)

# Cerrar la conexión
conexion.close()

# Mostrar los primeros registros del DataFrame
print(relaciones_df.head())
