import grpc
from concurrent import futures
import station_pb2
import station_pb2_grpc
import os 
import cassandra
from cassandra.cluster import Cluster
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from cassandra.query import ConsistencyLevel

class StationService(station_pb2_grpc.StationServicer):
    def __init__(self): 
        project = os.environ["PROJECT"]
        cluster = Cluster([f"{project}-db-1", f"{project}-db-2", f"{project}-db-3"])
        self.session = cluster.connect()

    # TODO: create schema for weather data;

        #1. drop a weather keyspace if it already exists
        self.session.execute("drop keyspace if exists weather")

        #2. create a weather keyspace with 3x replication
        self.session.execute("""
            CREATE KEYSPACE weather WITH replication = {
                'replication_factor': 3, 'class': 'SimpleStrategy'
            }
        """)

        #3. inside weather, create a station_record type containing two ints: tmin and tmax
        self.session.execute("use weather")
        self.session.execute("""
            CREATE TYPE station_record (
                tmin INT,
                tmax INT
            )
        """)

        #4. inside weather, create a stations table
        self.session.execute("""
            CREATE TABLE stations (
                id text,
                date date,
                name text STATIC,
                record station_record,
                PRIMARY KEY (id, date)
            ) WITH CLUSTERING ORDER BY (date ASC)
        """)

        self.spark = SparkSession.builder.appName("p6").getOrCreate()

    # TODO: load station data from ghcnd-stations.txt; 
        df = self.spark.read.text("ghcnd-stations.txt")
        wisc = df.select(
                        expr("substring(value, 1, 11)").alias("id"),
                        expr("substring(value, 39, 2)").alias("state"),
                        expr("substring(value, 42, 30)").alias("name")
                    ).where(col("state") == "WI").collect()

        for i in wisc:
            self.session.execute("""
                    INSERT INTO stations (id, name) VALUES (%s, %s)
                """, (i['id'], i['name'].strip()))
        
        self.temp_prep_stmt = self.session.prepare("""
                        INSERT INTO stations (id, date, record)
                        VALUES (?, ?, {tmin: ?, tmax: ?})
            """)
        self.temp_prep_stmt.consistency_level = ConsistencyLevel.ONE

        #Since RF=3, W=1, so to satisfy RF<R+W, we chose R=3
        self.max_prep_stmt = self.session.prepare("""
                         SELECT max(record.tmax) FROM stations WHERE id = ?
            """)
        self.max_prep_stmt.consistency_level = ConsistencyLevel.THREE


        # ============ Server Stated Successfully =============
        print("Server started") # Don't delete this line!


    def StationSchema(self, request, context):
        query = self.session.execute("DESCRIBE TABLE stations").one().create_statement
        return station_pb2.StationSchemaReply(schema=query, error="")

    def StationName(self, request, context):
        query = self.session.execute("""
                                          SELECT name 
                                          FROM stations 
                                          WHERE id = %s 
                    """, (request.station,)).one()

        return station_pb2.StationNameReply(name=query.name.strip(), error="")

    def RecordTemps(self, request, context):
        try:
            self.session.execute(
                self.temp_prep_stmt,
                (request.station, request.date, request.tmin, request.tmax)
            )
            return station_pb2.RecordTempsReply(error="")
        except (cassandra.Unavailable, cassandra.cluster.NoHostAvailable):
            return station_pb2.RecordTempsReply(error="unavailable") 

    def StationMax(self, request, context):
        try:
            query = self.session.execute(self.max_prep_stmt, 
                            (request.station,)).one()
            return station_pb2.StationMaxReply(tmax=query[0], error="")
        except (cassandra.Unavailable, cassandra.cluster.NoHostAvailable):
            return station_pb2.StationMaxReply(tmax=-1, error="unavailable")

def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=9),
        options=[("grpc.so_reuseport", 0)],
    )
    station_pb2_grpc.add_StationServicer_to_server(StationService(), server)
    server.add_insecure_port('0.0.0.0:5440')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()

