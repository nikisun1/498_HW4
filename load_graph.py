import pandas as pd
from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "Unicorn,123"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

df = pd.read_csv("taxi_trips_clean.csv")

def load_data(tx, row):
    tx.run("""
        MERGE (d:Driver {driver_id: $driver_id})
        MERGE (c:Company {name: $company})
        MERGE (a:Area {area_id: $dropoff_area})
        MERGE (d)-[:WORKS_FOR]->(c)
        CREATE (d)-[:TRIP {
            trip_id: $trip_id,
            fare: $fare,
            trip_seconds: $trip_seconds
        }]->(a)
    """,
    driver_id=str(row["driver_id"]),
    company=str(row["company"]),
    dropoff_area=int(row["dropoff_area"]),
    trip_id=str(row["trip_id"]),
    fare=float(row["fare"]),
    trip_seconds=int(row["trip_seconds"]))

with driver.session() as session:
    session.run("MATCH (n) DETACH DELETE n")
    for _, row in df.iterrows():
        session.execute_write(load_data, row)

driver.close()
print("Graph loaded successfully")
