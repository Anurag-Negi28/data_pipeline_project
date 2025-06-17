import yaml
from diagrams import Diagram, Cluster
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.compute import Server
from diagrams.onprem.client import Users
from diagrams.onprem.analytics import Spark

# Load your YAML architecture file
with open("architecture.yaml", "r") as f:
    arch = yaml.safe_load(f)

with Diagram("Lambda Architecture for Sales Pipeline", show=True, filename="lambda_pipeline", direction="TB"):

    # Input source
    source = Users("Sales CSV Files")

    # Batch Layer
    with Cluster("Batch Layer"):
        csv_scanner = Server("CSV Scanner")
        batch_etl = Spark("Batch ETL")
        validator = Server("Data Validator")
        batch_loader = Server("SQLite Loader")

        source >> csv_scanner >> batch_etl >> validator >> batch_loader

    # Speed Layer
    with Cluster("Speed Layer"):
        file_watcher = Server("File Watcher")
        stream_proc = Spark("Stream Processor")
        transformer = Server("Real-time Transformer")
        stream_loader = Server("Incremental Loader")

        source >> file_watcher >> stream_proc >> transformer >> stream_loader

    # Serving Layer
    with Cluster("Serving Layer"):
        database = PostgreSQL("SQLite DB")
        query_interface = Server("Query Interface")

        batch_loader >> database
        stream_loader >> database >> query_interface
