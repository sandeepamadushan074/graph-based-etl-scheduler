# Graph-Based ETL Pipeline Scheduling and Data Quality Optimization

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![NetworkX](https://img.shields.io/badge/NetworkX-DAG_Scheduling-green)
![Streamlit](https://img.shields.io/badge/Streamlit-Web_UI-red)
![SQLite](https://img.shields.io/badge/SQLite-Data_Warehouse-lightgrey)

## 📌 Project Overview

The **Graph-Based ETL Pipeline Scheduling and Data Quality Optimization** project is a comprehensive data engineering solution. It leverages advanced **Graph Theory** concepts (specifically Directed Acyclic Graphs, Topological Sorting, and Critical Path Analysis) to model, manage, and optimize the execution of an Extract, Transform, Validate, and Load (ETL) pipeline. 

Traditional ETL pipelines execute tasks in a rigid sequential order. This custom-built graph scheduler completely replaces that sequential bottleneck. By dynamically orchestrating tasks based on their exact dependencies, it guarantees maximum efficiency under limited processing capacity using multi-threading. Furthermore, the system incorporates strict **data quality validation gates** that act as circuit breakers, instantly blocking downstream operations if data anomalies are detected.

## ✨ Key Features

* **Custom DAG Scheduler:** Orchestrates pipeline tasks dynamically using Graph Theory. It automatically executes independent tasks in parallel via Python's `ThreadPoolExecutor`.
* **Critical-Path Prioritization (Proposed Method):** Calculates upward ranks (bottom-level weights) to prioritize the longest paths in the workflow. Ready tasks are dispatched to parallel workers based on this rank. This minimizes the overall makespan significantly.
* **Data Quality Gates (Fault Isolation):** Built-in validation checks stop corrupted data (such as duplicate IDs or missing values) from reaching the database. If a task fails, all its downstream dependent tasks are automatically blocked.
* **Automated Cycle Detection:** Prevents infinite execution loops by mathematically proving the absence of circular dependencies before execution.
* **Modular ETL Architecture:** Distinct, decoupled Python modules for Data Generation, Extraction, Transformation, Quality Validation, and Database Loading.
* **Interactive Web Dashboard:** A user-friendly Streamlit web UI to easily configure dataset volumes, adjust worker nodes, simulate data quality failures, and visualize benchmark metrics in real-time.

## 📂 Project Architecture & File Structure

The project is organized into a clean and highly modular architecture:

* `app.py`: The interactive Streamlit dashboard for running benchmarks and visualizing results.
* `main.py`: The primary CLI entry point for executing the experimental benchmark suite, generating performance charts, and demonstrating fault isolation.
* `scheduler.py`: The core graph orchestrator (`GraphETLScheduler`). It builds the DAG, calculates ranks, manages thread pools, and visualizes the network architecture.
* `pipeline_dag.py`: Defines the exact 14-Node Retail ETL DAG architecture.
* `data_generator.py`: Generates synthetic retail datasets (Customers, Products, Stores, Sales) with options to inject fatal quality errors.
* `extractor.py`: Extracts raw data from source CSV files.
* `transformer.py`: Cleans data by dropping duplicates, standardizing columns, and handling missing values.
* `quality_checks.py`: Validates structural integrity and acts as a strict gatekeeper before the loading phase.
* `loader.py`: Safely loads the validated, star-schema enriched data into a local SQLite Database (`graph_etl_warehouse.db`).
* `test_transformer.py`: Contains unit tests to verify the accuracy of the data cleaning rules.

## ⚙️ Installation & Setup

To run this project, you need Python 3 installed on your system. Follow these steps to set up the environment:

**1. Clone the repository:**
```bash
git clone <your-repository-url>
cd <repository-folder>
```

**2. Install dependencies:**
Install the required libraries using pip:
```bash
pip install pandas networkx matplotlib streamlit
```
*(Note: Database connections are handled using Python's built-in `sqlite3` module).*

## 🚀 How to Run the Project

You can interact with the system using either the Command Line Interface (CLI) or the Interactive Web Dashboard.

### Option 1: Run the Interactive Streamlit UI (Recommended)
Start the web application to run experiments through a graphical interface.
```bash
streamlit run app.py
```

### Option 2: Execute the CLI Benchmark Suite
Run the main script to generate synthetic data, visualize the 14-node DAG, execute tasks across all scheduling methods, save the performance chart, and demonstrate fault isolation.
```bash
python main.py
```

### Option 3: Run the Unit Tests
Verify that the transformation logic works correctly before executing the main pipeline.
```bash
python test_transformer.py
```

## 🔬 Scheduling Algorithms Evaluated

The automated experimental suite benchmarks three distinct execution strategies:

1. **Method A (Sequential Baseline):** Executes tasks sequentially in a basic topological order using a single worker.
2. **Method B (Basic Topological):** A parallel approach that dispatches tasks to a thread pool based on basic First-In-First-Out topological readiness. It does not consider execution weights.
3. **Method C (Proposed Critical-Path DAG):** The optimized approach that executes tasks in parallel but prioritizes them based on their computed Critical Path Rank.

## 📊 Performance Benchmark Results

Based on experimental evaluations across data volumes ranging from 10,000 to 100,000 rows, the Proposed Critical-Path DAG (Method C) consistently demonstrated superior scheduling efficiency.

- **Significant Speedup:** Method C achieved up to a 49% improvement in total execution time (Makespan) compared to the Sequential Baseline (Method A) for a 10,000-row dataset.
- **Optimal Parallelism:** By prioritizing the longest paths, Method C outperformed the Basic Topological approach (Method B) in almost all scenarios. This proves that priority-based scheduling avoids bottleneck delays in complex ETL pipelines.
- **Resilience:** The fault isolation mechanism successfully blocked downstream tasks (Data Warehouse Join and Load operations) automatically when intentional duplicate IDs were injected into the dataset. This ensured zero corrupted records entered the database.

## 🧮 Graph Theory Concepts Used
This project heavily relies on applied discrete mathematics:

* **Directed Acyclic Graph (DAG)**: The ETL pipeline is modeled as a graph $G = (V, E)$, where vertices ($V$) represent the ETL tasks (Extract, Transform, Validate, Load), and directed edges ($E$) represent the data dependencies.
* **Cycle Detection & Topological Sorting**: Before execution, the system checks for circular dependencies. If the graph is a valid DAG, topological sorting guarantees that every task has its required input data ready before it starts.
* **Critical Path Method (Upward Rank)**: This is the core of the proposed optimization (Method C). The scheduler calculates the "bottom-level" weight for each node using the recursive formula: 
  $$Rank(u) = t_u + \max_{v \in succ(u)} Rank(v)$$
  *(Where $t_u$ is the estimated execution time of task $u$, and $succ(u)$ represents its successors).*
  Ready tasks are dispatched to parallel workers sorted by this rank, ensuring that the most time-critical paths are processed first to minimize the overall execution time (Makespan).