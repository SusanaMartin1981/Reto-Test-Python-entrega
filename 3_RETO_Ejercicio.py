
# Ejercicio 3. Análisis de la empresa
# Vamos a crear un par de DataFrames uno con la información de las tablas de pedidos y clientes y otro con la información de productos, proveedores y detalles de los pedidos para poder hacer un estudio de la evolución de nuestra empresa y qué cosas podemos mejorar de esta.
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt

# Conexión a la base de datos PostgreSQL
conexion = psycopg2.connect(
    dbname="Northwind",      # Nombre de tu base de datos
    user="postgres",         # Usuario de PostgreSQL
    password="Susana",       # Contraseña de PostgreSQL
    host="localhost",        # Host donde está el servidor
    port="5432"              # Puerto del servidor
)

# Primera consulta: pedidos y clientes
consulta_pedidos_clientes = """
SELECT
    orders.order_id,
    orders.customer_id,
    orders.employee_id,
    orders.order_date,
    orders.required_date,
    orders.shipped_date,
    orders.ship_via,
    orders.freight,
    orders.ship_name,
    orders.ship_city,
    orders.ship_region,
    orders.ship_postal_code,
    orders.ship_country,
    customers.company_name,
    customers.contact_name,
    customers.contact_title,
    customers.address,
    customers.city,
    customers.region,
    customers.postal_code,
    customers.country,
    customers.phone,
    customers.fax
FROM
    orders
INNER JOIN
    customers
ON
    orders.customer_id = customers.customer_id;
"""

pedidos_clientes_df = pd.read_sql_query(consulta_pedidos_clientes, conexion)

# Segunda consulta: productos, proveedores y detalles de los pedidos
consulta_productos_proveedores = """
SELECT
    order_details.order_id,
    order_details.product_id,
    order_details.unit_price AS detail_unit_price,
    order_details.quantity,
    order_details.discount,
    products.supplier_id,
    products.category_id,
    products.quantity_per_unit,
    products.unit_price AS product_unit_price,
    products.units_in_stock,
    products.units_on_order,
    products.reorder_level,
    products.discontinued,
    suppliers.company_name,
    suppliers.contact_name,
    suppliers.contact_title,
    suppliers.address,
    suppliers.city,
    suppliers.region,
    suppliers.postal_code,
    suppliers.country,
    suppliers.phone,
    suppliers.fax,
    suppliers.homepage
FROM
    order_details
INNER JOIN
    products
ON
    order_details.product_id = products.product_id
INNER JOIN
    suppliers
ON
    products.supplier_id = suppliers.supplier_id;
"""

productos_proveedores_df = pd.read_sql_query(consulta_productos_proveedores, conexion)

# Cerrar la conexión
conexion.close()

# Mostrar los DataFrames
print("Pedidos y Clientes:")
print(pedidos_clientes_df.head())

print("\nProductos, Proveedores y Detalles de los Pedidos:")
print(productos_proveedores_df.head())

##EJERCICIO
# Haz un estudio de la evolución de los pedidos realizados a lo largo del tiempo. Para ello primero realiza la query necesaria para obtener los meses, años y pedidos durante cada mes. A continuación crea una línea temporal para ver dicha evolución


# Conexión a PostgreSQL
conexion = psycopg2.connect(
    dbname="Northwind",
    user="postgres",
    password="Susana",
    host="localhost",
    port="5432"
)

# Ejecutar la consulta SQL
consulta = """
SELECT 
    EXTRACT(YEAR FROM order_date) AS year,
    EXTRACT(MONTH FROM order_date) AS month,
    COUNT(order_id) AS total_orders
FROM 
    orders
GROUP BY 
    year, month
ORDER BY 
    year, month;
"""
datos = pd.read_sql_query(consulta, conexion)

# Cerrar la conexión
conexion.close()

# Crear una nueva columna para fechas
datos['date'] = pd.to_datetime(datos['year'].astype(int).astype(str) + '-' + datos['month'].astype(int).astype(str) + '-01')

# Visualización
plt.figure(figsize=(12, 6))
plt.plot(datos['date'], datos['total_orders'], marker='o')
plt.title('Evolución de Pedidos a lo Largo del Tiempo', fontsize=16)
plt.xlabel('Fecha', fontsize=14)
plt.ylabel('Total de Pedidos', fontsize=14)
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

#CONCLUSIÓN: Los pedidos van aumentando a lo largo de los años con una linea creciente constante.


##EJERCICIO
# Investiga cuáles son los países donde tenemos más ventas (País origen de la compañía). No es necesario realizar una query para obtener el DataFrame. A raíz de estos datos genera una columna con el continente a partir del siguiente diccionario dado:

# Con los datos disponibles del DataFrame existente que incluye las ventas y los países de origen, añadimos una columna que indique el continente al cual pertenece cada país utilizando el diccionario de continentes. Posteriormente, analizaremos la distribución de pedidos por continente y la visualizaremos

# Diccionario de continentes: Definimos el diccionario continentes con los paises agrupados por continente Europa y america.
continentes = {
    'Europe': ['Austria', 'Belgium', 'Denmark', 'Finland', 'France',
               'Germany', 'Ireland', 'Italy', 'Norway', 'Poland', 'Portugal',
               'Spain', 'Sweden', 'Switzerland', 'UK'],
    'America': ['Argentina', 'Brazil', 'Canada', 'Mexico', 'USA', 'Venezuela']
}

# Ejecutar la consulta SQL
#  df_original con las columnas necesarias
consulta = """
SELECT 
    ship_country AS country,
    COUNT(order_id) AS orders
FROM 
    orders
GROUP BY 
    ship_country
ORDER BY 
    orders DESC;
"""
df_original = pd.read_sql_query(consulta, conexion)  # DataFrame con los datos 


df = df_original.rename(columns={"ship_country": "country", "orders": "orders"})  
# Función para asignar el continente a cada país : esta función obtner_continente utiliza el diccionario para asignar el contienete correcto a cada pais en la columna country.
def obtener_continente(pais):
    for continente, paises in continentes.items():
        if pais in paises:
            return continente
    return 'Other'  # Para países no incluidos en el diccionario

# Añadir la columna de continente
df['continent'] = df['country'].apply(obtener_continente)

# Agrupar por continente y calcular la suma total de pedidos
distribucion_continente = df.groupby('continent')['orders'].sum()

# Mostrar la distribución
print(distribucion_continente)

# Visualización: Gráfico de barras
plt.figure(figsize=(8, 5))
distribucion_continente.plot(kind='bar', color=['blue', 'green'], alpha=0.7)
plt.title('Distribución de Pedidos por Continente', fontsize=16)
plt.xlabel('Continente', fontsize=14)
plt.ylabel('Total de Pedidos', fontsize=14)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()

#CONCLUSION: SE VENDE MUCHO MAS EN EUROPA: Europa +-500 frente a AMERICA  +- 330. 
# Los paises dond se mas se vende es Germany =122 empatando con USA segido de Brais con 83 y Francia con 77, UK ya baja a 56



# Sabemos que algunos pedidos han llegado con retraso, además hay pedidos que no ha sido registrada su llegada.Investiga si la compañía de transporte está relacionada con ello o no. Realiza un boxplot para ver la diferencia de rango intercuartílico.



import pandas as pd
import psycopg2
import plotly.express as px

# Conexión a PostgreSQL
conexion = psycopg2.connect(
    dbname="Northwind",
    user="postgres",
    password="Susana",
    host="localhost",
    port="5432"
)

# Consulta SQL para obtener los datos necesarios
consulta = """
SELECT 
    o.required_date,
    o.shipped_date,
    s.company_name AS transporter
FROM 
    orders o
JOIN 
    shippers s
ON 
    o.ship_via = s.shipper_id;
"""

# Crear el DataFrame
df = pd.read_sql_query(consulta, conexion)

# Cerrar la conexión
conexion.close()

# Calcular el retraso en días
df['required_date'] = pd.to_datetime(df['required_date'])
df['shipped_date'] = pd.to_datetime(df['shipped_date'])
df['delay'] = (df['shipped_date'] - df['required_date']).dt.days

# Filtrar pedidos con fecha de envío no registrada
df = df[df['shipped_date'].notnull()]  # Excluir pedidos sin fecha de llegada

# Crear el boxplot interactivo con Plotly
fig = px.box(
    df,
    x='delay',
    y='transporter',
    color='transporter',
    title='Retraso en Días por Compañía de Transporte',
    labels={'delay': 'Días de Retraso', 'transporter': 'Compañía de Transporte'},
    orientation='h',  # Horizontal
    width=800,
    height=600
)

# Mostrar el gráfico interactivo
fig.show()
# Calcular estadísticas descriptivas agrupadas por transportista
estadisticas = df.groupby('transporter')['delay'].describe()

# Mostrar las estadísticas
print(estadisticas)

## CONCLUSION: las tres compañias de trasportes tienen un comportamiento muy parecido con lo que a priori no podríamos decir que una es mejor que la otra en cuanto a retrasos ya que son muuy parecidos, siendo algo peor la empresa United Package.



##EJERCICIO
# Hay bastante diferencia entre el precio pagado en cada pedido. Averigüa la distribución media del precio del pedido por país de procedencia del cliente. Realiza la visualización que creas más conveniente para sacar conclusiones
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2

# Conexión a la base de datos PostgreSQL
conexion = psycopg2.connect(
    dbname="Northwind",
    user="postgres",
    password="Susana",
    host="localhost",
    port="5432"
)

# Consulta SQL para obtener los datos necesarios
consulta = """
SELECT 
    od.order_id,
    o.ship_country,
    od.unit_price,
    od.quantity,
    od.discount
FROM 
    order_details od
JOIN 
    orders o
ON 
    od.order_id = o.order_id;
"""

# Crear el DataFrame
df = pd.read_sql_query(consulta, conexion)

# Cerrar la conexión
conexion.close()

# Calcular el precio total pagado por pedido
df['total_price'] = df['unit_price'] * df['quantity'] * (1 - df['discount'])

# Calcular la media del precio total por país
media_por_pais = df.groupby('ship_country')['total_price'].mean().sort_values()

# Mostrar las medias
print(media_por_pais)

# Visualización: Gráfico de barras
plt.figure(figsize=(12, 6))
media_por_pais.plot(kind='bar', color='skyblue', alpha=0.8)
plt.title('Media del Precio de Pedido por País', fontsize=16)
plt.xlabel('País', fontsize=14)
plt.ylabel('Precio Medio del Pedido', fontsize=14)
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

#CONCLUSIÓN: Hay dos dos paises que destacan por encimma de todos, en primer lugar Austria(1024), seguido de Irlanda(908)

##EJERCICIO
# Investiga si existen clientes que no hayan pedido nunca. ¿Qué porcentaje de clientes no tienen pedidos registrados?
import pandas as pd
import psycopg2

# Conexión a PostgreSQL
conexion = psycopg2.connect(
    dbname="Northwind",
    user="postgres",
    password="Susana",
    host="localhost",
    port="5432"
)

# Consulta SQL para identificar clientes sin pedidos
consulta = """
SELECT 
    c.customer_id
FROM 
    customers c
LEFT JOIN 
    orders o
ON 
    c.customer_id = o.customer_id
WHERE 
    o.customer_id IS NULL;
"""

# Crear el DataFrame con los clientes sin pedidos
clientes_sin_pedidos_df = pd.read_sql_query(consulta, conexion)

# Contar el número de clientes sin pedidos
numero_clientes_sin_pedidos = len(clientes_sin_pedidos_df)

# Consulta SQL para obtener el número total de clientes
consulta_total_clientes = "SELECT COUNT(*) AS total_clientes FROM customers;"
total_clientes_df = pd.read_sql_query(consulta_total_clientes, conexion)

# Número total de clientes
total_clientes = total_clientes_df['total_clientes'][0]

# Cerrar la conexión
conexion.close()

# Calcular el porcentaje de clientes sin pedidos
porcentaje_sin_pedidos = (numero_clientes_sin_pedidos / total_clientes) * 100

# Mostrar el resultado
print(f"El porcentaje de clientes que no tienen pedidos registrados es: {porcentaje_sin_pedidos:.2f}%")

#CONCLUSIÓN: El porcentaje de clientes que no tienen pedidos registrados es :2.20%

#EJERCICIO:
# Estudia los productos más demandados e investiga cuáles corre prisa hacer reestock (Los que quedan 20 o menos y no hay unidades pedidas). Realiza la visualización que creas más conveniente para sacar conclusiones.
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2

# Conexión a la base de datos PostgreSQL
conexion = psycopg2.connect(
    dbname="Northwind",
    user="postgres",
    password="Susana",
    host="localhost",
    port="5432"
)

# Consulta SQL para obtener datos de productos y demandas
consulta = """
SELECT 
    p.product_id,
    p.product_name,
    p.units_in_stock,
    p.units_on_order,
    SUM(od.quantity) AS total_demand
FROM 
    products p
LEFT JOIN 
    order_details od
ON 
    p.product_id = od.product_id
GROUP BY 
    p.product_id, p.product_name, p.units_in_stock, p.units_on_order
ORDER BY 
    total_demand DESC;
"""

# Crear el DataFrame
df = pd.read_sql_query(consulta, conexion)

# Cerrar la conexión
conexion.close()

# Filtrar productos con bajo stock y sin unidades pedidas
productos_reabastecer = df[(df['units_in_stock'] <= 20) & (df['units_on_order'] == 0)]

# Visualización 1: Productos más demandados
plt.figure(figsize=(12, 6))
df.head(10).plot(
    x='product_name', 
    y='total_demand', 
    kind='bar', 
    color='skyblue', 
    alpha=0.8, 
    legend=False
)
plt.title('Top 10 Productos Más Demandados', fontsize=16)
plt.xlabel('Producto', fontsize=14)
plt.ylabel('Demanda Total', fontsize=14)
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

# Visualización 2: Productos que necesitan reabastecimiento
plt.figure(figsize=(12, 6))
productos_reabastecer.plot(
    x='product_name', 
    y='units_in_stock', 
    kind='bar', 
    color='lightcoral', 
    alpha=0.8, 
    legend=False
)
plt.title('Productos con Stock Crítico y Sin Pedido Pendiente', fontsize=16)
plt.xlabel('Producto', fontsize=14)
plt.ylabel('Unidades en Stock', fontsize=14)
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

#CONCLUSION: Hay que tener cuidado con el Camembert Pierrot porque es el producto mas demandado y su stok esta por debajo de 20 y no se ha pedido, y otro producto tambien muy reclamado y con poco stok es la Guaraná Fantastica.