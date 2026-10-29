import time
import networkx as nx
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor

class GraphETLScheduler:
    def __init__(self, max_workers=3):
        self.graph = nx.DiGraph()
        self.max_workers = max_workers
        self.task_funcs = {}
        self.est_weights = {}
        self.critical_ranks = {}
        self.layers = {}

    def add_task(self, task_id, func, est_time, layer, dependencies=None):
        self.graph.add_node(task_id)
        self.task_funcs[task_id] = func
        self.est_weights[task_id] = est_time
        self.layers[task_id] = layer

        if dependencies:
            for dep in dependencies:
                self.graph.add_edge(dep, task_id)

    def compute_critical_path_ranks(self):
        topo_order = list(nx.topological_sort(self.graph))
        for node in reversed(topo_order):
            successors = list(self.graph.successors(node))
            if not successors:
                self.critical_ranks[node] = self.est_weights[node]
            else:
                self.critical_ranks[node] = self.est_weights[node] + max(
                    self.critical_ranks[succ] for succ in successors
                )
        return self.critical_ranks

    def visualize_dag(self, save_path="etl_dag_architecture.png"):
        self.compute_critical_path_ranks()
        plt.figure(figsize=(15, 8))

        layer_groups = {}
        for node, layer in self.layers.items():
            layer_groups.setdefault(layer, []).append(node)

        pos = {}
        for layer, nodes in layer_groups.items():
            y_positions = np.linspace(1.0, -1.0, len(nodes))
            for y, node in zip(y_positions, nodes):
                pos[node] = (layer * 2.5, y)

        color_map = []
        for node in self.graph.nodes:
            if node.startswith("Ext"):
                color_map.append("#4FC3F7")
            elif node.startswith("Trf"):
                color_map.append("#FFB74D")
            elif node.startswith("Val"):
                color_map.append("#FFF176")
            elif node.startswith("Join"):
                color_map.append("#BA68C8")
            else:
                color_map.append("#81C784")

        labels = {
            n: f"{n}\n(Est: {self.est_weights[n]}s | Rank: {self.critical_ranks[n]:.1f})"
            for n in self.graph.nodes
        }

        nx.draw(
            self.graph, pos, labels=labels, with_labels=True,
            node_color=color_map, edge_color="#455A64",
            node_size=3400, font_size=8, font_weight="bold",
            arrows=True, arrowsize=18, width=1.8
        )

        plt.title("Multi-Source Retail ETL Pipeline DAG (With Critical Path Ranks)", fontsize=14, fontweight="bold")
        plt.tight_layout()
        plt.savefig(save_path, dpi=300)
        print(f"[+] Saved high-resolution DAG visualization to '{save_path}'")
        plt.close()

    def execute(self, mode="proposed"):
        if not nx.is_directed_acyclic_graph(self.graph):
            raise ValueError("Pipeline graph contains a cycle! Execution aborted.")

        self.compute_critical_path_ranks()
        workers = 1 if mode == "sequential" else self.max_workers

        completed = set()
        failed = set()
        blocked = set()
        
        # New: Tracking variables for running tasks
        running_futures = {}  
        running_tasks = set() 
        task_metrics = {}

        start_pipeline = time.perf_counter()

        with ThreadPoolExecutor(max_workers=workers) as executor:
            while len(completed) + len(failed) + len(blocked) < len(self.graph.nodes):
                
                # 1. Identify tasks whose upstream dependencies have failed/blocked
                for node in self.graph.nodes:
                    if node not in completed and node not in failed and node not in blocked and node not in running_tasks:
                        preds = list(self.graph.predecessors(node))
                        if any(p in failed or p in blocked for p in preds):
                            blocked.add(node)
                            print(f"    [QUALITY GATE BLOCK] Downstream task '{node}' blocked due to upstream failure.")

                # 2. Find ready tasks
                ready_tasks = [
                    node for node in self.graph.nodes
                    if node not in completed and node not in failed and node not in blocked and node not in running_tasks
                    and all(p in completed for p in self.graph.predecessors(node))
                ]

                # 3. Prioritize tasks based on scheduling algorithm
                if mode == "proposed":
                    ready_tasks.sort(key=lambda n: self.critical_ranks[n], reverse=True)

                # 4. Dispatch tasks to available worker threads
                while ready_tasks and len(running_futures) < workers:
                    next_task = ready_tasks.pop(0)
                    t_start = time.perf_counter()
                    future = executor.submit(self.task_funcs[next_task])
                    running_futures[future] = (next_task, t_start)
                    running_tasks.add(next_task) 

                # 5. Collect completed futures
                if running_futures:
                    done_futures = [f for f in running_futures if f.done()]
                    if not done_futures:
                        time.sleep(0.005)
                        continue

                    for future in done_futures:
                        task_name, t_start = running_futures.pop(future)
                        running_tasks.remove(task_name) 
                        duration = time.perf_counter() - t_start
                        
                        try:
                            future.result()
                            completed.add(task_name)
                            task_metrics[task_name] = ("SUCCESS", duration)
                        except Exception as e:
                            failed.add(task_name)
                            task_metrics[task_name] = ("FAILED", duration)
                            print(f"    [!] Task '{task_name}' FAILED: {e}")

        makespan = time.perf_counter() - start_pipeline
        return {
            "mode": mode,
            "makespan": makespan,
            "completed": len(completed),
            "failed": len(failed),
            "blocked": len(blocked),
            "task_metrics": task_metrics
        }