import clickhouse_connect
import pandas as pd
import random

# 1. Cargar el CSV
df = pd.read_csv('datasets/Death_Rates1900-2013.csv')

# Limpiar nombres de columnas para evitar comas y espacios
df.columns = df.columns.str.strip()
df.columns = df.columns.str.replace(',', '')

# 2. Crear una columna "average_age" con valores entre 45 y 85 (simulados)
df["average_age"] = [random.randint(45, 85) for _ in range(len(df))]

# 3. Conectar a ClickHouse sin contraseña
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default'
    # password se omite porque no tienes contraseña
)

# 4. Crear la tabla (si existe, la borra)
client.command('DROP TABLE IF EXISTS death_rates')
client.command('''
CREATE TABLE death_rates (
    year UInt16,
    cause String,
    death_rate Float32,
    average_age Float32
) ENGINE = MergeTree
ORDER BY (year, cause)
''')

# 5. Insertar los datos
data = list(zip(
    df['Year'],
    df['Leading Causes'],
    df['Age Adjusted Death Rate'],
    df['average_age']
))

client.insert('death_rates', data, column_names=['year', 'cause', 'death_rate', 'average_age'])

print("¡Datos cargados en ClickHouse con éxito!")
