import clickhouse_connect
from neo4j import GraphDatabase
import pandas as pd

# --- CONEXIÓN A CLICKHOUSE ---
clickhouse = clickhouse_connect.get_client(host='localhost', port=8123)

# --- CONEXIÓN A NEO4J ---
neo4j_uri = "neo4j://127.0.0.1:7687"
neo4j_user = "neo4j"
neo4j_password = "pascual1"
neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

# --- MAPEOS DE NOMBRES ENTRE AMBAS BASES ---
CAUSE_MAP = {
    "Stroke": "Stroke",
    "Cancer": "Cancer",
    "Influenza and pneumonia": "Influenza and Pneumonia",
    "Diseases of Heart": "Heart Disease",
    "Unintentional Injuries": "Accidents"
}

# --- FUNCIONES ---

def get_common_causes():
    query = "SELECT DISTINCT cause FROM death_rates"
    clickhouse_causes = set(row[0] for row in clickhouse.query(query).result_rows)
    
    with neo4j_driver.session() as session:
        result = session.run("MATCH (c:Cause) RETURN DISTINCT c.name AS cause")
        neo4j_causes = set(record["cause"] for record in result)
    
    neo4j_causes_mapped = [c for c in CAUSE_MAP if CAUSE_MAP[c] in clickhouse_causes and c in neo4j_causes]
    return sorted(neo4j_causes_mapped)

def get_common_years(cause_neo4j, cause_clickhouse):
    query = f"SELECT DISTINCT year FROM death_rates WHERE cause = '{cause_clickhouse}' ORDER BY year"
    clickhouse_years = set(row[0] for row in clickhouse.query(query).result_rows)

    with neo4j_driver.session() as session:
        result = session.run(
            """
            MATCH (s:State)-[r:REPORTED]->(c:Cause {name: $cause})
            RETURN DISTINCT r.year AS year
            ORDER BY year
            """, cause=cause_neo4j)
        neo4j_years = set(record["year"] for record in result if record["year"] is not None)
    
    return sorted(clickhouse_years.intersection(neo4j_years))

def get_national_death_rate(cause_clickhouse, year):
    query = f"""
        SELECT AVG(death_rate) AS avg_rate
        FROM death_rates
        WHERE cause = '{cause_clickhouse}' AND year = {year}
    """
    result = clickhouse.query(query)
    if result.result_rows:
        return result.result_rows[0][0]
    return None

def get_state_deaths_from_neo4j(cause_neo4j, year):
    with neo4j_driver.session() as session:
        query = """
            MATCH (s:State)-[r:REPORTED]->(c:Cause {name: $cause})
            WHERE r.year = $year
            RETURN s.name AS state, r.deaths AS deaths
            ORDER BY deaths DESC
        """
        result = session.run(query, cause=cause_neo4j, year=year)
        return pd.DataFrame([dict(record) for record in result])

def mostrar_menu(lista_opciones, titulo):
    print(f"\nSeleccione {titulo}:")
    for i, item in enumerate(lista_opciones, 1):
        print(f"{i}. {item}")
    print("Ingrese el número correspondiente (o 'q' para volver al menú): ", end="")

def export_combined_summary_to_csv():
    print("\nExtrayendo datos de Neo4j y ClickHouse...")

    causas = get_common_causes()
    rows = []

    for causa_neo in causas:
        causa_ch = CAUSE_MAP[causa_neo]
        años = get_common_years(causa_neo, causa_ch)

        for año in años:
            # Neo4j: total muertes en todos los estados para causa y año
            with neo4j_driver.session() as session:
                result = session.run("""
                    MATCH (s:State)-[r:REPORTED]->(c:Cause {name: $causa})
                    WHERE s.name <> 'United States' AND r.year = $año
                    RETURN r.deaths AS deaths
                """, causa=causa_neo, año=año)
                muertes = [record["deaths"] for record in result if record["deaths"] is not None]
                total_neo4j = sum(muertes) if muertes else 0
                estados = len(muertes)

            # ClickHouse: tasa promedio nacional
            query = f"""
                SELECT AVG(death_rate) AS avg_rate
                FROM death_rates
                WHERE cause = '{causa_ch}' AND year = {año}
            """
            result = clickhouse.query(query)
            avg_ch = result.result_rows[0][0] if result.result_rows else None

            rows.append({
                "causa": causa_neo,
                "año": año,
                "muertes_totales_neo4j": total_neo4j,
                "tasa_promedio_nacional_clickhouse": round(avg_ch, 2) if avg_ch else None,
                "estados_reportados": estados
            })

    df = pd.DataFrame(rows)
    df = df.sort_values(by=["causa", "año"])
    df.to_csv("resumen_integrado_causas.csv", index=False)
    print("CSV generado: 'resumen_integrado_causas.csv'")

# --- PROGRAMA PRINCIPAL ---

def main():
    while True:
        print("\n=== MENÚ PRINCIPAL ===")
        print("1. Analizar causa y año")
        print("2. Exportar resumen combinado (Neo4j + ClickHouse)")
        print("3. Salir")
        opcion = input("Seleccione una opción: ").strip()

        if opcion == "1":
            causas = get_common_causes()
            if not causas:
                print("No hay causas comunes.")
                continue

            mostrar_menu(causas, "la causa para analizar")
            op = input().strip()
            if op.lower() == 'q':
                continue
            if not op.isdigit() or not (1 <= int(op) <= len(causas)):
                print("Opción inválida.")
                continue
            causa_neo = causas[int(op) - 1]
            causa_ch = CAUSE_MAP[causa_neo]

            años = get_common_years(causa_neo, causa_ch)
            if not años:
                print("No hay años comunes.")
                continue

            mostrar_menu(años, "el año para analizar")
            op_año = input().strip()
            if op_año.lower() == 'q':
                continue
            if not op_año.isdigit() or not (1 <= int(op_año) <= len(años)):
                print("Año inválido.")
                continue
            año = años[int(op_año) - 1]

            print(f"\nAnálisis de: {causa_neo} ({causa_ch}) en el año {año}")

            national_rate = get_national_death_rate(causa_ch, año)
            if national_rate is not None:
                print(f"\nTasa nacional promedio: {national_rate:.2f} muertes por 100,000 habitantes\n")
            else:
                print("No se encontró la causa en ClickHouse.")
                continue

            df = get_state_deaths_from_neo4j(causa_neo, año)
            df = df[df['state'] != 'United States']
            df = df.groupby('state', as_index=False)['deaths'].sum()
            df = df.sort_values(by='deaths', ascending=False)

            if not df.empty:
                print("Top muertes por estado:\n")
                print(df.head(51))
                nombre_csv = f"salida_{causa_neo.replace(' ', '_')}_{año}.csv"
                df.to_csv(nombre_csv, index=False)
                print(f"\nResultados guardados en '{nombre_csv}'")
            else:
                print("No se encontraron datos en Neo4j.")

        elif opcion == "2":
            export_combined_summary_to_csv()

        elif opcion == "3":
            print("Saliendo...")
            break
        else:
            print("Opción inválida.")

if __name__ == "__main__":
    main()
