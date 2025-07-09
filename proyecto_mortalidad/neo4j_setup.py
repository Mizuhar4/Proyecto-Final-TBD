from neo4j import GraphDatabase
import pandas as pd

# Configuración de conexión
uri = "neo4j://127.0.0.1:7687"
user = "neo4j"
password = "pascual1"

# Cargar y limpiar el CSV
df = pd.read_csv("datasets/leading_cause_death.csv")
df = df[['STATE', 'CAUSE_NAME', 'YEAR', 'DEATHS']]
df = df.dropna()
df = df[df['DEATHS'].apply(lambda x: str(x).isdigit())]
df['DEATHS'] = df['DEATHS'].astype(int)
df['YEAR'] = df['YEAR'].astype(int)

# Conexión con Neo4j
driver = GraphDatabase.driver(uri, auth=(user, password))

# Función para insertar los datos con año en la relación REPORTED
def insert_data(tx, state, cause, year, deaths):
    tx.run("""
        MERGE (s:State {name: $state})
        MERGE (c:Cause {name: $cause})
        MERGE (y:Year {value: $year})
        MERGE (s)-[r:REPORTED {year: $year}]->(c)
        SET r.deaths = $deaths
        MERGE (c)-[:OCCURRED_IN]->(y)
    """, state=state, cause=cause, year=year, deaths=deaths)

# Ejecutar la carga
with driver.session() as session:
    for _, row in df.iterrows():
        session.execute_write(insert_data, row['STATE'], row['CAUSE_NAME'], row['YEAR'], row['DEATHS'])

print("¡Datos cargados en Neo4j con éxito!")
driver.close()
