# Graph-Based ETL Pipeline Scheduling and Data Quality Optimization

## Overview
The **Graph-Based ETL Pipeline Scheduling and Data Quality Optimization** project is a research-oriented data engineering solution. It leverages **Graph Theory**—specifically Directed Acyclic Graphs (DAGs) and Topological Sorting—to model, manage, and optimize the execution order of an Extract, Transform, Validate, and Load (ETL) pipeline. 

By orchestrating tasks based on their exact dependencies rather than a rigid sequential order, this custom-built scheduler ensures maximum efficiency under limited processing capacity. Furthermore, it integrates strict data quality gates, halting downstream operations if validations fail, thereby guaranteeing high data integrity in the final data warehouse.

## Key Features
* **Custom DAG Scheduler**: Orchestrates pipeline tasks dynamically using Graph Theory instead of relying on heavyweight frameworks like Apache Airflow.
* **Dependency-Aware Execution**: Ensures tasks only run when all upstream dependencies are successfully completed.
* **Automated Cycle Detection**: Prevents infinite execution loops by mathematically proving the absence of circular dependencies before execution.
* **Data Quality Gates**: Built-in validation checks that act as circuit breakers to prevent corrupted or null data from reaching the database.
* **Modular ETL Design**: Distinct and decoupled modules for Extraction, Transformation, Validation, and Loading.

## Project Structure
The project consists of 6 primary Python scripts, ensuring a clean and modular architecture:

* `extractor.py` - Extracts raw data from source systems (e.g., CSV files) and loads it into a Pandas DataFrame.
* `transformer.py` - Cleans the extracted data by removing duplicates, standardizing column names, handling null values, and stripping whitespaces.
* `quality_checks.py` - Validates the transformed data for structural integrity and quality. Throws errors to stop downstream execution if anomalies are found.
* `loader.py` - Loads the validated, clean data into the target SQLite Database (`graph_etl_warehouse.db`) using SQLAlchemy.
* `PaythonDraft_01.py` - The core orchestrator and main entry point of the project. It builds the DAG using `networkx`, performs cycle detection and topological sorting, visualizes the task graph, and safely executes the pipeline.
* `test_transformer.py` - Contains unit tests to verify the accuracy of the data cleaning rules applied in the transformation phase.

## Prerequisites
To run this project, you need Python 3 installed on your system along with the following libraries:
* `pandas` (for data manipulation)
* `networkx` (for graph-based scheduling and analysis)
* `matplotlib` (for DAG visualization)
* `sqlalchemy` (for database connections)

You can install all dependencies using the following command:
```bash
pip install pandas networkx matplotlib sqlalchemy
```

## Installation & Execution

1. **Clone the repository** (or navigate to the project directory):
```bash
git clone <your-repository-url>
cd <repository-folder>
```

2. **Run the Unit Tests** (Optional but recommended):
Verify that the transformation logic is working correctly before running the pipeline.
```bash
python test_transformer.py
```

3. **Execute the ETL Pipeline**:
Run the main orchestrator script. It will automatically generate dummy data, visualize the DAG, execute tasks in topological order, and load the results into an SQLite database.
```bash
python PaythonDraft_01.py
```

## Graph Theory Concepts Used
This project heavily relies on applied discrete mathematics:

* **Directed Acyclic Graph (DAG)**: The ETL pipeline is modeled as a graph $G = (V, E)$, where vertices ($V$) represent the ETL tasks (Extract, Transform, Validate, Load), and directed edges ($E$) represent the dependencies (e.g., Transform depends on Extract).
* **Cycle Detection**: Before execution, a Depth-First Search (DFS) based algorithm checks the graph for cycles. If a cycle exists (e.g., Task A depends on B, and B depends on A), the pipeline safely aborts, preventing an infinite loop.
* **Topological Sorting**: If the graph is a valid DAG, the scheduler computes a linear ordering of its vertices such that for every directed edge $u \rightarrow v$, vertex $u$ comes before $v$ in the ordering. This guarantees that every task has its required input data ready before it starts.