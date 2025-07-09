# Proyecto Final - Taller de Bases de Datos

### Autores: BenjamÍn Fernández & Maximiliano Sepúlveda  
**Tema:** Análisis de causas de mortalidad en Estados Unidos (1900–2013) utilizando integración de bases de datos no relacionales (Neo4j y ClickHouse).

---

## Estructura del Proyecto

```bash
Proyecto-Final-TBD/
├── proyecto_mortalidad/
│     ├── datasets/
│     │   ├── Death_Rates1900-2013.csv
│     │   └── leading_cause_death.csv
│     ├── clickhouse_setup.py
│     ├── neo4j_setup.py
│     ├── integration.py
│     └── informe.PDF
└──README.md
```

## Requisitos

Antes de ejecutar los scripts, asegúrate de tener instalado:

- **Neo4j Desktop** o servidor local en `localhost:7687`
- **ClickHouse** (`clickhouse-client`) --> `version 25.6.2.5`
- **Python 3.1** con entorno virtual `venv`
- Librerías necesarias:
  ```bash
  pip install clickhouse-connect pandas neo4j

## Instrucciones de Uso

### 1 Activar tu entorno virtual (opcional pero recomendado, para instalar las librerias)

- Usar el siguiente comando
  ```bash
  python3 -m venv myenv
  source myenv/bin/activate

### 2 Cargar datos en ClickHouse

- Abre el archivo clickhouse_setup.py y asegúrate de que el dataset esté en datasets/.

- Ejecuta el script:
  ```bash
  python clickhouse_setup.py

- Este script carga el archivo Death_Rates1900-2013.csv en una tabla llamada death_rates, usando la base de datos por defecto (default) de ClickHouse. (En este ejemplo no se usa contraseña en ClickHouse)

### 3 Cargar datos en Neo4j

- Asegúrate de tener Neo4j Desktop iniciado.

- Abre el archivo neo4j_setup.py.

- Ejecuta el script:
  ```bash
  python neo4j_setup.py

- Este script carga el archivo leading_cause_death.csv a Neo4j, creando los nodos:

  - (State {name})
  - (Cause {name})
  - (Year {value})

- Y las relaciones:
  - (State)-[:REPORTED {deaths}]->(Cause)
  - (Cause)-[:OCCURRED_IN]->(Year)

-Puedes visualizar los grafos en: http://localhost:7474
(Usuario: neo4j | Contraseña: pascual1)

### 4 Ejecutar integración de datos

- Ejecuta el script:
  ```bash
  python integration.py

- Este script hace lo siguiente:
  - Cruza los datos entre ClickHouse y Neo4j.
  - Calcula la tasa nacional promedio de muertes para una causa y año desde ClickHouse.
  - Muestra las muertes por estado desde Neo4j.
  - Filtra registros como "United States" para dejar solo estados reales.
  - Agrupa y muestra el Top 10 de estados más afectados.
  - Exporta los resultados a un CSV con el nombre: "salida_<Causa>_<Año>.csv"
 
## Tecnologías Usadas

- ClickHouse: Motor columnar para datos de gran volumen.
- Neo4j: Base de datos orientada a grafos, ideal para relaciones entre entidades.
- Python: Lenguaje de integración y análisis.
- Pandas: Procesamiento de datos.
- Cypher: Lenguaje de consultas para Neo4j.

## Objetivo del Proyecto

Integrar dos motores de bases de datos no relacionales, extraer información desde ambas, y cruzar los datos usando Python para generar visualizaciones y análisis relevantes sobre las causas de mortalidad en EE. UU.
