import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from data_generator import generate_retail_datasets
from scheduler import GraphETLScheduler
from pipeline_dag import build_retail_etl_dag

def run_experimental_benchmarks():
    volumes = [10000, 50000, 100000]
    methods = [
        ("sequential", "Method A: Sequential Baseline"),
        ("topological", "Method B: Basic Topological"),
        ("proposed", "Method C: Proposed Critical-Path DAG")
    ]

    results_list = []

    print("\n" + "=" * 70)
    print(" RUNNING EXPERIMENTAL EVALUATION ACROSS DATA VOLUMES & METHODS")
    print("=" * 70)

    for rows in volumes:
        print(f"\n>>> Generating Synthetic Retail Dataset ({rows:,} Sales Rows)...")
        generate_retail_datasets(rows, inject_fatal_error=False)

        for mode_key, mode_label in methods:
            print(f"\n--- Running {mode_label} ({rows:,} rows) ---")
            state = {}
            scheduler = GraphETLScheduler(max_workers=3)
            build_retail_etl_dag(scheduler, state)

            res = scheduler.execute(mode=mode_key)
            results_list.append({
                "Dataset_Rows": rows,
                "Method": mode_label,
                "Mode_Key": mode_key,
                "Makespan_Sec": round(res["makespan"], 4),
                "Completed_Tasks": res["completed"],
                "Blocked_Tasks": res["blocked"]
            })
            print(f"==> Finished {mode_label} | Makespan: {res['makespan']:.3f}s")

    results_df = pd.DataFrame(results_list)

    print("\n" + "=" * 70)
    print(" EXPERIMENTAL RESULTS SUMMARY TABLE")
    print("=" * 70)
    pivot_df = results_df.pivot(index="Dataset_Rows", columns="Method", values="Makespan_Sec")
    pivot_df["Improvement_vs_Seq (%)"] = (
        (pivot_df["Method A: Sequential Baseline"] - pivot_df["Method C: Proposed Critical-Path DAG"])
        / pivot_df["Method A: Sequential Baseline"] * 100
    ).round(2)
    pivot_df["Improvement_vs_Topo (%)"] = (
        (pivot_df["Method B: Basic Topological"] - pivot_df["Method C: Proposed Critical-Path DAG"])
        / pivot_df["Method B: Basic Topological"] * 100
    ).round(2)

    print(pivot_df.to_string())
    pivot_df.to_csv("experimental_results_summary.csv")
    print("\n[+] Saved benchmark table to 'experimental_results_summary.csv'")

    plt.figure(figsize=(10, 6))
    x = np.arange(len(volumes))
    width = 0.25

    m_a = results_df[results_df["Mode_Key"] == "sequential"]["Makespan_Sec"].values
    m_b = results_df[results_df["Mode_Key"] == "topological"]["Makespan_Sec"].values
    m_c = results_df[results_df["Mode_Key"] == "proposed"]["Makespan_Sec"].values

    plt.bar(x - width, m_a, width, label="Method A: Sequential Baseline", color="#EF5350")
    plt.bar(x, m_b, width, label="Method B: Basic Topological (Parallel)", color="#42A5F5")
    plt.bar(x + width, m_c, width, label="Method C: Proposed Critical-Path DAG", color="#66BB6A")

    plt.xlabel("Dataset Volume (Number of Sales Records)", fontsize=11, fontweight="bold")
    plt.ylabel("Total Execution Time / Makespan (Seconds)", fontsize=11, fontweight="bold")
    plt.title("ETL Pipeline Scheduling Performance Comparison", fontsize=13, fontweight="bold")
    plt.xticks(x, [f"{v:,} Rows" for v in volumes])
    plt.legend()
    plt.grid(axis="y", linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.savefig("performance_comparison_chart.png", dpi=300)
    print("[+] Saved performance comparison chart to 'performance_comparison_chart.png'")
    plt.show()

    print("\n" + "=" * 70)
    print(" TESTING DATA QUALITY VALIDATION GATE (FAULT ISOLATION DEMO)")
    print("=" * 70)
    generate_retail_datasets(10000, inject_fatal_error=True)
    fault_state = {}
    fault_scheduler = GraphETLScheduler(max_workers=3)
    build_retail_etl_dag(fault_scheduler, fault_state)
    fault_res = fault_scheduler.execute(mode="proposed")
    print(
        f"==> Fault Test Result: {fault_res['completed']} Tasks Succeeded | "
        f"{fault_res['failed']} Failed (Val_Prod) | "
        f"{fault_res['blocked']} Downstream Tasks Blocked (Join_DW, Load_DW)!"
    )

if __name__ == "__main__":
    demo_state = {}
    demo_scheduler = GraphETLScheduler(max_workers=3)
    build_retail_etl_dag(demo_scheduler, demo_state)
    print("[*] Visualizing 14-Node Multi-Source ETL Pipeline DAG...")
    demo_scheduler.visualize_dag()

    run_experimental_benchmarks()