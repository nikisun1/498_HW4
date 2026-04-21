from flask import Flask, request, jsonify
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round

app = Flask(__name__)

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "Unicorn,123"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
def get_spark():
    return SparkSession.builder.appName("HW4Spark").getOrCreate()

@app.route("/area-stats", methods=["GET"])
def area_stats():
    area_id = int(request.args.get("area_id"))
    spark = get_spark()

    df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)

    result = (
        df.filter(col("dropoff_area") == area_id)
          .groupBy("dropoff_area")
          .agg(
              count("*").alias("trip_count"),
              round(avg("fare"), 2).alias("avg_fare"),
              round(avg("trip_seconds"), 0).alias("avg_trip_seconds")
          )
          .collect()
    )

    if not result:
        return jsonify({
            "area_id": area_id,
            "trip_count": 0,
            "avg_fare": 0,
            "avg_trip_seconds": 0
        })

    row = result[0]
    return jsonify({
        "area_id": area_id,
        "trip_count": row["trip_count"],
        "avg_fare": float(row["avg_fare"]),
        "avg_trip_seconds": int(row["avg_trip_seconds"])
    })

@app.route("/top-pickup-areas", methods=["GET"])
def top_pickup_areas():
    n = int(request.args.get("n", 5))
    spark = get_spark()

    df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)

    result = (
        df.groupBy("pickup_area")
          .agg(count("*").alias("trip_count"))
          .orderBy(col("trip_count").desc())
          .limit(n)
          .collect()
    )

    areas = [
        {"pickup_area": row["pickup_area"], "trip_count": row["trip_count"]}
        for row in result
    ]

    return jsonify({"areas": areas})

@app.route("/company-compare", methods=["GET"])
def company_compare():
    company1 = request.args.get("company1")
    company2 = request.args.get("company2")
    spark = get_spark()

    df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)
    df = df.withColumn("fare_per_minute", col("fare") / (col("trip_seconds") / 60.0))
    df.createOrReplaceTempView("trips")

    query = f"""
        SELECT
            company,
            COUNT(*) AS trip_count,
            ROUND(AVG(fare), 2) AS avg_fare,
            ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute,
            ROUND(AVG(trip_seconds), 0) AS avg_trip_seconds
        FROM trips
        WHERE company IN ('{company1}', '{company2}')
        GROUP BY company
    """

    result = spark.sql(query).collect()

    if len(result) < 2:
        return jsonify({"error": "one or more companies not found"})

    comparison = []
    for row in result:
        comparison.append({
            "company": row["company"],
            "trip_count": row["trip_count"],
            "avg_fare": float(row["avg_fare"]),
            "avg_fare_per_minute": float(row["avg_fare_per_minute"]),
            "avg_trip_seconds": int(row["avg_trip_seconds"])
        })

    return jsonify({"comparison": comparison})

@app.route("/graph-summary", methods=["GET"])
def graph_summary():
    with driver.session() as session:
        driver_count = session.run("MATCH (d:Driver) RETURN count(d) AS count").single()["count"]
        company_count = session.run("MATCH (c:Company) RETURN count(c) AS count").single()["count"]
        area_count = session.run("MATCH (a:Area) RETURN count(a) AS count").single()["count"]
        trip_count = session.run("MATCH ()-[t:TRIP]->() RETURN count(t) AS count").single()["count"]

    return jsonify({
        "driver_count": driver_count,
        "company_count": company_count,
        "area_count": area_count,
        "trip_count": trip_count
    })

@app.route("/top-companies", methods=["GET"])
def top_companies():
    n = int(request.args.get("n", 5))
    query = """
    MATCH (d:Driver)-[:WORKS_FOR]->(c:Company)
    MATCH (d)-[:TRIP]->(:Area)
    RETURN c.name AS name, count(*) AS trip_count
    ORDER BY trip_count DESC
    LIMIT $n
    """
    with driver.session() as session:
        result = session.run(query, n=n)
        companies = [{"name": r["name"], "trip_count": r["trip_count"]} for r in result]
    return jsonify({"companies": companies})

@app.route("/high-fare-trips", methods=["GET"])
def high_fare_trips():
    area_id = int(request.args.get("area_id"))
    min_fare = float(request.args.get("min_fare"))

    query = """
    MATCH (d:Driver)-[t:TRIP]->(a:Area {area_id: $area_id})
    WHERE t.fare > $min_fare
    RETURN t.trip_id AS trip_id, t.fare AS fare, d.driver_id AS driver_id
    ORDER BY t.fare DESC
    """
    with driver.session() as session:
        result = session.run(query, area_id=area_id, min_fare=min_fare)
        trips = [{"trip_id": r["trip_id"], "fare": r["fare"], "driver_id": r["driver_id"]} for r in result]
    return jsonify({"trips": trips})

@app.route("/co-area-drivers", methods=["GET"])
def co_area_drivers():
    driver_id = request.args.get("driver_id")

    query = """
    MATCH (d1:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)<-[:TRIP]-(d2:Driver)
    WHERE d1 <> d2
    RETURN d2.driver_id AS driver_id, count(DISTINCT a) AS shared_areas
    ORDER BY shared_areas DESC
    """
    with driver.session() as session:
        result = session.run(query, driver_id=driver_id)
        co_area_drivers = [{"driver_id": r["driver_id"], "shared_areas": r["shared_areas"]} for r in result]
    return jsonify({"co_area_drivers": co_area_drivers})

@app.route("/avg-fare-by-company", methods=["GET"])
def avg_fare_by_company():
    query = """
    MATCH (d:Driver)-[:WORKS_FOR]->(c:Company)
    MATCH (d)-[t:TRIP]->(:Area)
    RETURN c.name AS name, round(avg(t.fare), 2) AS avg_fare
    ORDER BY avg_fare DESC
    """
    with driver.session() as session:
        result = session.run(query)
        companies = [{"name": r["name"], "avg_fare": r["avg_fare"]} for r in result]
    return jsonify({"companies": companies})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
