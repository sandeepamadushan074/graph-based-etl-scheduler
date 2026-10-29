import streamlit as st
import pandas as pd
import os
from PaythonDraft_01 import (
    GraphETLScheduler,
    build_retail_etl_dag,
    generate_retail_datasets
)

st.set_page_config(page_title="Graph ETL Scheduler", layout="wide")

st.title("Graph-Based ETL Pipeline Orchestrator & Optimizer")
st.write("Interactive System for Dependency-Aware Scheduling, Critical-Path Optimization, and Data Quality Gates")

# Sidebar controls
st.sidebar.header("Experiment Configuration")
data_volume = st.sidebar.selectbox("Select Dataset Volume (Rows)", [10000, 50000, 100000])
max_workers = st.sidebar.slider("Parallel Worker Capacity", min_value=1, max_value=4, value=3)
inject_error = st.sidebar.checkbox("Simulate Data Quality Failure (Products)", value=False)

if st.sidebar.button("Run Pipeline & Benchmark"):
    with st.spinner("Generating synthetic datasets and running graph algorithms..."):
        generate_retail_datasets(data_volume, inject_fatal_error=inject_error)
        
        methods = [
            ("sequential", "Method A: Sequential Baseline"),
            ("topological", "Method B: Basic Topological"),
            ("proposed", "Method C: Proposed Critical-Path DAG")
        ]
        
        results = []
        task_logs = {}
        
        for mode_key, mode_label in methods:
            state = {}
            scheduler = GraphETLScheduler(max_workers=max_workers)
            build_retail_etl_dag(scheduler, state)
            res = scheduler.execute(mode=mode_key)
            
            results.append({
                "Scheduling Method": mode_label,
                "Makespan (Seconds)": round(res["makespan"], 4),
                "Completed Tasks": res["completed"],
                "Failed Tasks": res["failed"],
                "Blocked Tasks": res["blocked"]
            })
            if mode_key == "proposed":
                task_logs = res["task_metrics"]
                
        res_df = pd.DataFrame(results)
        
        # Calculate improvement
        seq_time = res_df.loc[0, "Makespan (Seconds)"]
        prop_time = res_df.loc[2, "Makespan (Seconds)"]
        improvement = round(((seq_time - prop_time) / seq_time) * 100, 2)

        # Display KPI Metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Sequential Time", f"{seq_time} s")
        col2.metric("Proposed DAG Time", f"{prop_time} s")
        col3.metric("Speed Improvement", f"{improvement} %")
        col4.metric("Quality Gate Status", "BLOCKED" if inject_error else "PASSED")

        st.subheader("1. Performance Benchmark Results")
        st.dataframe(res_df, use_container_width=True)

        st.subheader("2. Execution Time Comparison")
        st.bar_chart(res_df.set_index("Scheduling Method")["Makespan (Seconds)"])

        st.subheader("3. Node-Level Execution Metrics (Proposed Method)")
        log_rows = [{"Task Node": k, "Status": v[0], "Duration (s)": round(v[1], 4)} for k, v in task_logs.items()]
        st.table(pd.DataFrame(log_rows))

st.subheader("Pipeline DAG Architecture")
if os.path.exists("etl_dag_architecture.png"):
    st.image("etl_dag_architecture.png", caption="14-Node Multi-Source ETL Directed Acyclic Graph")
else:
    st.info("Run PaythonDraft_01.py once and close the popup window to save the architecture diagram.")