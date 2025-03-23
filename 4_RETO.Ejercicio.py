# Ejercicio 4. Queries Avanzadas

# Nuestro jefe acaba de venir y nos ha hecho una serie de peticiones sobre la base de datos que tenemos que poder contestar.

#EJERCICIO.
# Quiere saber cuándo fue la última vez que se pidió un producto de cada catgoría.

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

# Crear el cursor para ejecutar consultas
cursor = conexion.cursor()

# Consulta SQL para obtener la última fecha de pedido por categoría
consulta = """
SELECT c.category_name, MAX(o.order_date) AS ultima_fecha_pedido
FROM orders AS o
JOIN order_details AS od ON o.order_id = od.order_id
JOIN products AS p ON od.product_id = p.product_id
JOIN categories AS c ON p.category_id = c.category_id
GROUP BY c.category_name;
"""

# Ejecutar la consulta
cursor.execute(consulta)

# Obtener los resultados y convertirlos en un DataFrame para fácil manejo
resultados = cursor.fetchall()
columnas = ['Categoria', 'Ultima_Fecha_Pedido']
df_resultados = pd.DataFrame(resultados, columns=columnas)

# Cerrar el cursor y la conexión
cursor.close()
conexion.close()

# Mostrar los resultados
print(df_resultados)

#CONCLUSION: Hay siete categorias y todas ellas fueron pedidas por ultima vez 1998-05-06


#EJERCICIO.
# Necesita saber si existe algún producto que nunca se haya vendido por su precio original.
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

# Crear el cursor para ejecutar consultas
cursor = conexion.cursor()

# Consulta SQL
consulta = """
SELECT p.product_id, p.product_name
FROM products AS p
WHERE NOT EXISTS (
    SELECT 1
    FROM order_details AS od
    WHERE od.product_id = p.product_id
    AND od.unit_price = p.unit_price
    AND od.discount = 0
);
"""

# Ejecutar la consulta
cursor.execute(consulta)

# Obtener los resultados y convertirlos en un DataFrame
resultados = cursor.fetchall()
columnas = ['Product_ID', 'Product_Name']
df_resultados = pd.DataFrame(resultados, columns=columnas)

# Cerrar el cursor y la conexión
cursor.close()
conexion.close()


#CONCLUSIÓN: Producto_ID  15 Product_Name Genen Shouyu


#EJERCICIO.
# Quiere tener toda la información necesaria para poder identificar un tipo de producto. En concreto, tienen especial interés por los productos con categoría "Confections". Devuelve el ID del producto, el nombre del producto y su ID de categoría.
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

# Crear el cursor para ejecutar consultas
cursor = conexion.cursor()

# Consulta SQL para productos de la categoría "Confections"
consulta = """
SELECT p.product_id, p.product_name, p.category_id
FROM products AS p
JOIN categories AS c ON p.category_id = c.category_id
WHERE c.category_name = 'Confections';
"""

# Ejecutar la consulta
cursor.execute(consulta)

# Obtener los resultados y convertirlos en un DataFrame
resultados = cursor.fetchall()
columnas = ['Product_ID', 'Product_Name', 'Category_ID']
df_resultados = pd.DataFrame(resultados, columns=columnas)

# Cerrar el cursor y la conexión
cursor.close()
conexion.close()

# Mostrar los resultados
print(df_resultados)


#CONCLUSIÓN: Hay 12 productos en la categoria Confections cuyo id identificativo es 3.


#EJERCICIO

#Quiere saber si existe algún proveedor del que pueda prescindir ya que todos los productos que tiene se encuentran descontinuados.
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

# Crear el cursor para ejecutar consultas
cursor = conexion.cursor()

# Consulta SQL para identificar proveedores prescindibles
consulta = """
SELECT s.supplier_id, s.company_name
FROM suppliers AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM products AS p
    WHERE p.supplier_id = s.supplier_id
    AND p.discontinued = 0
);
"""

# Ejecutar la consulta
cursor.execute(consulta)

# Obtener los resultados y convertirlos en un DataFrame
resultados = cursor.fetchall()
columnas = ['Supplier_ID', 'Company_Name']
df_resultados = pd.DataFrame(resultados, columns=columnas)

# Cerrar el cursor y la conexión
cursor.close()
conexion.close()

# Mostrar los resultados
print(df_resultados)


#CONCLUSIÓN: SI podria prescindir de supplier_id  número 10 company_name Refrescos Americanas LTA


#EJERCICIO
# Extraer los clientes que compraron mas de 30 articulos "Chai" en un único pedido

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

# Crear el cursor para ejecutar consultas
cursor = conexion.cursor()

# Consulta SQL
consulta = """
SELECT cu.customer_id, cu.company_name
FROM customers AS cu
JOIN orders AS o ON cu.customer_id = o.customer_id
JOIN order_details AS od ON o.order_id = od.order_id
JOIN products AS p ON od.product_id = p.product_id
WHERE p.product_name = 'Chai'
AND od.quantity > 30;
"""

# Ejecutar la consulta
cursor.execute(consulta)

# Obtener los resultados y convertirlos en un DataFrame
resultados = cursor.fetchall()
columnas = ['Customer_ID', 'Company_Name']
df_resultados = pd.DataFrame(resultados, columns=columnas)

# Cerrar el cursor y la conexión
cursor.close()
conexion.close()

# Mostrar los resultados
print(df_resultados)

#CONCLUSIÓN: Hay 9 clientes que comparon mas de 30 arculos "Chai" en un único peido. Ejemplo Quick-shop, 



#EJERCICIO
#Indica los clientes cuya suma total de carga en los pedidos sea mayor de 1000
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

# Crear el cursor para ejecutar consultas
cursor = conexion.cursor()

# Consulta SQL para clientes con carga total mayor a 1000
consulta = """
SELECT o.customer_id, SUM(o.freight) AS total_freight
FROM orders AS o
GROUP BY o.customer_id
HAVING SUM(o.freight) > 1000;
"""

# Ejecutar la consulta
cursor.execute(consulta)

# Obtener los resultados y convertirlos en un DataFrame
resultados = cursor.fetchall()
columnas = ['Customer_ID', 'Total_Freight']
df_resultados = pd.DataFrame(resultados, columns=columnas)

# Cerrar el cursor y la conexión
cursor.close()
conexion.close()

# Mostrar los resultados
print(df_resultados)

#CONCLUSIÓN: Hay 17 clientes clientes cuya suma total de carga en los pedidos sea mayor de 1000



#EJERCICIO
# Desde recursos humanos nos piden seleccionar los nombres de las ciudades con 5 o más empleadas de cara a estudiar la apertura de nuevas oficinas.
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

# Crear el cursor para ejecutar consultas
cursor = conexion.cursor()

# Consulta SQL actualizada para incluir solo empleadas
consulta = """
SELECT city, COUNT(employee_id) AS total_empleadas
FROM employees
WHERE title_of_courtesy IN ('Ms.', 'Mrs.')
GROUP BY city
HAVING COUNT(employee_id) >= 5;
"""

# Ejecutar la consulta
cursor.execute(consulta)

# Obtener los resultados y convertirlos en un DataFrame
resultados = cursor.fetchall()
columnas = ['City', 'Total_Empleadas']
df_resultados = pd.DataFrame(resultados, columns=columnas)

# Cerrar el cursor y la conexión
cursor.close()
conexion.close()

# Mostrar los resultados
print(df_resultados)

#CONCLUSIÓN :No hay ninguna ciudad que cumpla la condicion.

