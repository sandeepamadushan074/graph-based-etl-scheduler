import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
import os

# Import ETL functions from the other files
from extractor import extract_from_csv
from transformer import transform_data
from quality_checks import run_quality_checks
from loader import load_to_database

class TaskScheduler:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.task_functions = {}

    def add_task(self, task_name, func=None, dependencies=None):
        if task_name in self.graph:
            raise ValueError(f"Task '{task_name}' already exists.")
        
        self.graph.add_node(task_name)
        self.task_functions[task_name] = func
        
        if dependencies:
            for dep in dependencies:
                if dep not in self.graph:
                    raise ValueError(f"Dependency '{dep}' does not exist.")
                self.graph.add_edge(dep, task_name)

    def detect_cycle(self):
        try:
            cycle = nx.find_cycle(self.graph, orientation="original")
            return True, cycle
        except nx.NetworkXNoCycle:
            return False, []

    def topological_sort(self):
        has_cycle, _ = self.detect_cycle()
        if has_cycle:
            print("[ERROR] Cycle detected! Cannot perform topological sort.")
            return None
        return list(nx.topological_sort(self.graph))

    def visualize(self):
        plt.figure(figsize=(8, 5))
        nx.draw(self.graph, with_labels=True, node_color='skyblue', 
                edge_color='gray', node_size=2500, font_size=10, font_weight='bold', arrows=True)
        plt.title("ETL Pipeline Directed Acyclic Graph (DAG)")
        plt.show()

    def execute_pipeline(self):
        execution_order = self.topological_sort()
        if not execution_order:
            return

        print("\n")
        print(" Starting Graph-Based ETL Pipeline")
        print(f"Execution Order: {' -> '.join(execution_order)}")
        print("\n")

        task_status = {}

        for task in execution_order:
            dependencies = list(self.graph.predecessors(task))
            
            # Ensure all upstream dependencies were successfully completed
            can_run = all(task_status.get(dep, False) for dep in dependencies)

            if not can_run:
                print(f"[SKIP] Skipping '{task}' because a dependent upstream task failed.")
                task_status[task] = False
                continue

            print(f"---- Executing Task: {task} ----")
            try:
                func = self.task_functions[task]
                if func:
                    func()
                task_status[task] = True
                print(f"[SUCCESS] Task '{task}' completed successfully.\n")
            except Exception as e:
                print(f"[FAILED] Task '{task}' encountered an error: {e}\n")
                task_status[task] = False

        print(" ETL Pipeline Execution Finished")



# --- Pipeline Execution Setup ---

# Dictionary to hold data between tasks
data_store = {}

def task_extract():
    data_store['raw_data'] = extract_from_csv("sample_sales.csv")

def task_transform():
    data_store['clean_data'] = transform_data(data_store['raw_data'], "Sales Dataset")

def task_validate():
    is_valid = run_quality_checks(data_store['clean_data'], "Sales Dataset")
    if not is_valid:
        raise ValueError("Data Quality Validation Failed. Pipeline stopped.")

def task_load():
    load_to_database(data_store['clean_data'], "sales_fact_table")

if __name__ == "__main__":
    # 1. Create a dummy CSV file for the pipeline to process
    dummy_data = {
        "Customer ID": [101, 102, 102, 104],
        " Product Name ": ["Laptop ", "Mouse", "Mouse", None],
        "Price": [1200.0, 25.0, 25.0, None]
    }
    df = pd.DataFrame(dummy_data)
    df.to_csv("sample_sales.csv", index=False)

    # 2. Initialize the Graph Scheduler
    scheduler = TaskScheduler()

    # 3. Add tasks and define dependencies to build the DAG
    scheduler.add_task("Extract", task_extract)
    scheduler.add_task("Transform", task_transform, dependencies=["Extract"])
    scheduler.add_task("Validate", task_validate, dependencies=["Transform"])
    scheduler.add_task("Load", task_load, dependencies=["Validate"])

    # 4. Visualize the dependencies (DAG)
    print("[*] Generating Pipeline Visualization...")
    scheduler.visualize()

    # 5. Run the ETL Pipeline based on Topological Order
    scheduler.execute_pipeline()
    
    # Clean up the dummy file after execution
    if os.path.exists("sample_sales.csv"):
        os.remove("sample_sales.csv")