# main.py
import os
import subprocess
import click
from dotenv import load_dotenv
import tempfile
import shutil
import requests, zipfile, io
import sys


# Load environment variables from .env file at the start
load_dotenv()

def run_flash_perf(algorithm, data_path=None):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MACHINE_LIST = [2, 4, 8, 16]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "config"
    template_path = os.path.join(yaml_dir, "flash-mpijob-template.yaml")

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    # if data_path is None:
    #     if algorithm == "sssp":
    #         data_path = os.path.abspath("sample_data/flash_sssp_sample_graph/")
    #     else:
    #         data_path = os.path.abspath("sample_data/flash_sample_graph/")

    if algorithm == "k-core-search":
        ALGORITHM_PARAMETER_ = 3
    elif algorithm == "clique":
        ALGORITHM_PARAMETER_ = 5
    else:
        ALGORITHM_PARAMETER_ = 1

    VOLUMES_BLOCK = ''
    VOLUME_MOUNTS_BLOCK = ''

    if algorithm != 'sssp':
        VOLUMES_BLOCK = f"volumes:\n          - name: flash-data\n            hostPath:\n              path: {data_path}\n              type: Directory" if data_path else ""
        VOLUME_MOUNTS_BLOCK = f"volumeMounts:\n            - name: flash-data\n              mountPath: /opt/data" if data_path else ""
    else:
        VOLUMES_BLOCK = f"volumes:\n          - name: flash-data\n            hostPath:\n              path: {data_path}\n              type: Directory" if data_path else ""
        VOLUME_MOUNTS_BLOCK = f"volumeMounts:\n            - name: flash-data\n              mountPath: /opt/data_sssp" if data_path else ""
    
    try:
        with open(template_path, 'r') as f:
            template_str = f.read()
    except FileNotFoundError:
        print(f"[ERROR] Template file not found: {template_path}")
        return

    for thread in THREAD_LIST:
        params = {
            "SLOTS_PER_WORKER": thread,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": 1,
            "MPIRUN_NP": thread,
            "ALGORITHM": algorithm,
            "ALGORITHM_PARAMETER": ALGORITHM_PARAMETER_,
            "SINGLE_MACHINE": 1,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        
        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))
        
        yaml_path = os.path.join(yaml_dir, f"flash-mpijob-{algorithm}-single.yaml")
        
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)

            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            
            print(f"[INFO] Submitting MPIJob: {algorithm} single-machine, threads={thread}")
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run([kubectl, "wait", "--for=condition=Succeeded", "mpijob/flash-mpijob", "--timeout=100m"], check=True)
            dataset_name = os.path.basename(os.path.normpath(data_path))
            log_file = os.path.join(output_dir, f"flash_{algorithm}-{dataset_name}-n1-p{thread}.log")
            subprocess.run([kubectl, "logs", "job/flash-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    for machines in MACHINE_LIST:
        params = {
            "SLOTS_PER_WORKER": 32,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": machines,
            "MPIRUN_NP": machines * 32,
            "ALGORITHM": algorithm,
            "ALGORITHM_PARAMETER": ALGORITHM_PARAMETER_,
            "SINGLE_MACHINE": 0,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        
        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"flash-mpijob-{algorithm}-multi.yaml")
        
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)

            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            
            print(f"[INFO] Submitting MPIJob: {algorithm} multi-machine, machines={machines}")
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run([kubectl, "wait", "--for=condition=Succeeded", "mpijob/flash-mpijob", "--timeout=10m"], check=True)
            dataset_name = os.path.basename(os.path.normpath(data_path))
            log_file = os.path.join(output_dir, f"flash_{algorithm}-{dataset_name}-n{machines}-p32.log")
            subprocess.run([kubectl, "logs", "job/flash-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    print("[INFO] ✅ All experiments completed.")


def run_ligra_perf(algorithm, data_path=None):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "config"
    template_path = os.path.join(yaml_dir, "ligra-mpijob-template.yaml")

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    if algorithm == 'BellmanFord':
        VOLUMES_BLOCK = f"volumes:\n          - name: ligra-data\n            hostPath:\n              path: {data_path}\n              type: File" if data_path else ""
        VOLUME_MOUNTS_BLOCK = f"volumeMounts:\n            - name: ligra-data\n              mountPath: /opt/data/graph_sssp.txt" if data_path else ""
    else:
        VOLUMES_BLOCK = f"volumes:\n          - name: ligra-data\n            hostPath:\n              path: {data_path}\n              type: File" if data_path else ""
        VOLUME_MOUNTS_BLOCK = f"volumeMounts:\n            - name: ligra-data\n              mountPath: /opt/data/graph.txt" if data_path else ""
    


    try:
        with open(template_path, 'r') as f:
            template_str = f.read()
    except FileNotFoundError:
        print(f"[ERROR] Template file not found: {template_path}")
        return

    for thread in THREAD_LIST:
        params = {
            "SLOTS_PER_WORKER": thread,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": 1,
            "MPIRUN_NP": thread,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 1,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"ligra-mpijob-{algorithm}-single.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(f"[INFO] Submitting Ligra MPIJob: {algorithm} threads={thread}")
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run([kubectl, "wait", "--for=condition=Succeeded", "mpijob/ligra-mpijob", "--timeout=10m"], check=True)
            log_file = os.path.join(output_dir, f"ligra_{algorithm}-n1-p{thread}.log")
            subprocess.run([kubectl, "logs", "job/ligra-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    print("[INFO] ✅ Ligra experiments completed.")

def run_grape_perf(algorithm, data_path=None):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MACHINE_LIST = [2, 4, 8, 16]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "config"
    template_path = os.path.join(yaml_dir, "grape-mpijob-template.yaml")

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    if algorithm == 'sssp':
        VOLUMES_BLOCK = f"volumes:\n          - name: grape-data-v\n            hostPath:\n              path: {data_path}.v\n              type: File\n          - name: grape-data-e\n            hostPath:\n              path: {data_path}.e\n              type: File" if data_path else ""
        VOLUME_MOUNTS_BLOCK = f"volumeMounts:\n            - name: grape-data-v\n              mountPath: /opt/data/graph_sssp.v\n            - name: grape-data-e\n              mountPath: /opt/data/graph_sssp.e" if data_path else ""

    else:
        VOLUMES_BLOCK = f"volumes:\n          - name: grape-data-v\n            hostPath:\n              path: {data_path}.v\n              type: File\n          - name: grape-data-e\n            hostPath:\n              path: {data_path}.e\n              type: File" if data_path else ""
        VOLUME_MOUNTS_BLOCK = f"volumeMounts:\n            - name: grape-data-v\n              mountPath: /opt/data/graph.v\n            - name: grape-data-e\n              mountPath: /opt/data/graph.e" if data_path else ""


    try:
        with open(template_path, 'r') as f:
            template_str = f.read()
    except FileNotFoundError:
        print(f"[ERROR] Template file not found: {template_path}")
        return

    for thread in THREAD_LIST:
        params = {
            "SLOTS_PER_WORKER": thread,
            "HOST_PATH_V": data_path+".v",
            "HOST_PATH_E": data_path+".e",
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": 1,
            "MPIRUN_NP": thread,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 1,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"grape-mpijob-{algorithm}-single.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(f"[INFO] Submitting MPIJob: {algorithm} single-machine, threads={thread}")
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run([kubectl, "wait", "--for=condition=Succeeded", "mpijob/grape-mpijob", "--timeout=10m"], check=True)
            log_file = os.path.join(output_dir, f"grape_{algorithm}-n1-p{thread}.log")
            subprocess.run([kubectl, "logs", "job/grape-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    for machines in MACHINE_LIST:
        params = {
            "SLOTS_PER_WORKER": 32,
            "HOST_PATH_V": data_path+".v",
            "HOST_PATH_E": data_path+".e",
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": machines,
            "MPIRUN_NP": machines * 32,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 0,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"grape-mpijob-{algorithm}-multi.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(f"[INFO] Submitting MPIJob: {algorithm} multi-machine, machines={machines}")
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run([kubectl, "wait", "--for=condition=Succeeded", "mpijob/grape-mpijob", "--timeout=10m"], check=True)
            log_file = os.path.join(output_dir, f"grape_{algorithm}-n{machines}-p32.log")
            subprocess.run([kubectl, "logs", "job/grape-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    print("[INFO] ✅ Grape experiments completed.")

def run_pregel_perf(algorithm, data_path=None):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MACHINE_LIST = [2, 4, 8, 16]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "config"
    template_path = os.path.join(yaml_dir, "pregel-mpijob-template.yaml")

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    VOLUMES_BLOCK = f"- name: pregeldata\n              hostPath:\n                path: {data_path}\n                type: File" if data_path else ""
    VOLUME_MOUNTS_BLOCK = f"- name: pregeldata\n                  mountPath: /opt/data/graph.txt" if data_path else ""


    for thread in THREAD_LIST:
        params = {
            "SLOTS_PER_WORKER": thread,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": 1,
            "MPIRUN_NP": thread,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 1,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        try:
            with open(template_path, 'r') as f:
                template_str = f.read()
        except FileNotFoundError:
            print(f"[ERROR] Template file not found: {template_path}")
            return

        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"pregel-mpijob-{algorithm}-single.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(f"[INFO] Submitting Pregel+ MPIJob: {algorithm} single-machine, threads={thread}")
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run([kubectl, "wait", "--for=condition=Succeeded", "mpijob/pregel-mpijob", "--timeout=10m"], check=True)
            log_file = os.path.join(output_dir, f"pregel_{algorithm}-n1-p{thread}.log")
            subprocess.run([kubectl, "logs", "job/pregel-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    for machines in MACHINE_LIST:
        params = {
            "SLOTS_PER_WORKER": 32,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": machines,
            "MPIRUN_NP": machines * 32,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 0,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        try:
            with open(template_path, 'r') as f:
                template_str = f.read()
        except FileNotFoundError:
            print(f"[ERROR] Template file not found: {template_path}")
            return

        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"pregel-mpijob-{algorithm}-multi.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(f"[INFO] Submitting Pregel+ MPIJob: {algorithm} multi-machine, machines={machines}")
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run([kubectl, "wait", "--for=condition=Succeeded", "mpijob/pregel-mpijob", "--timeout=10m"], check=True)
            log_file = os.path.join(output_dir, f"pregel_{algorithm}-n{machines}-p32.log")
            subprocess.run([kubectl, "logs", "job/pregel-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    print("[INFO] ✅ Pregel+ experiments completed.")

def run_gthinker_perf(algorithm, data_path=None):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MACHINE_LIST = [2, 4, 8, 16]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "config"
    template_path = os.path.join(yaml_dir, "gthinker-mpijob-template.yaml")

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)


    VOLUMES_BLOCK = f"- name: gthinkerdata\n              hostPath:\n                path: {data_path}\n                type: File" if data_path else ""
    VOLUME_MOUNTS_BLOCK = f"- name: gthinkerdata\n                  mountPath: /opt/data/graph.txt" if data_path else ""


    for thread in THREAD_LIST:
        params = {
            "SLOTS_PER_WORKER": thread,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": 1,
            "MPIRUN_NP": thread,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 1,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        try:
            with open(template_path, 'r') as f:
                template_str = f.read()
        except FileNotFoundError:
            print(f"[ERROR] Template file not found: {template_path}")
            return

        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"gthinker-mpijob-{algorithm}-single.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(f"[INFO] Submitting Gthinker MPIJob: {algorithm} single-machine, threads={thread}")
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run([kubectl, "wait", "--for=condition=Succeeded", "mpijob/gthinker-mpijob", "--timeout=10m"], check=True)
            log_file = os.path.join(output_dir, f"gthinker_{algorithm}-n1-p{thread}.log")
            subprocess.run([kubectl, "logs", "job/gthinker-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    for machines in MACHINE_LIST:
        params = {
            "SLOTS_PER_WORKER": 32,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": machines,
            "MPIRUN_NP": machines * 32,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 0,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        try:
            with open(template_path, 'r') as f:
                template_str = f.read()
        except FileNotFoundError:
            print(f"[ERROR] Template file not found: {template_path}")
            return

        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"gthinker-mpijob-{algorithm}-multi.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(f"[INFO] Submitting Gthinker MPIJob: {algorithm} multi-machine, machines={machines}")
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run([kubectl, "wait", "--for=condition=Succeeded", "mpijob/gthinker-mpijob", "--timeout=10m"], check=True)
            log_file = os.path.join(output_dir, f"gthinker_{algorithm}-n{machines}-p32.log")
            subprocess.run([kubectl, "logs", "job/gthinker-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    print("[INFO] ✅ Gthinker experiments completed.")

def run_powergraph_perf(algorithm, data_path=None):
    THREAD_LIST = [1, 2, 4, 8, 16, 32]
    MACHINE_LIST = [2, 4, 8, 16]
    MEMORY = "100Gi"
    CPU = "32"
    yaml_dir = "config"
    template_path = os.path.join(yaml_dir, "powergraph-mpijob-template.yaml")

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    VOLUMES_BLOCK = f"- name: graphlabdata\n              hostPath:\n                path: {data_path}\n                type: File" if data_path else ""
    VOLUME_MOUNTS_BLOCK = f"- name: graphlabdata\n                  mountPath: /opt/data/graph.txt" if data_path else ""


    for thread in THREAD_LIST:
        params = {
            "SLOTS_PER_WORKER": thread,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": 1,
            "MPIRUN_NP": thread,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 1,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        try:
            with open(template_path, 'r') as f:
                template_str = f.read()
        except FileNotFoundError:
            print(f"[ERROR] Template file not found: {template_path}")
            return

        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"powergraph-mpijob-{algorithm}-single.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(f"[INFO] Submitting PowerGraph MPIJob: {algorithm} single-machine, threads={thread}")
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run([kubectl, "wait", "--for=condition=Succeeded", "mpijob/powergraph-mpijob", "--timeout=10m"], check=True)
            log_file = os.path.join(output_dir, f"powergraph_{algorithm}-n1-p{thread}.log")
            subprocess.run([kubectl, "logs", "job/powergraph-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    for machines in MACHINE_LIST:
        params = {
            "SLOTS_PER_WORKER": 32,
            "HOST_PATH": data_path,
            "CPU": CPU,
            "MEMORY": MEMORY,
            "REPLICAS": machines,
            "MPIRUN_NP": machines * 32,
            "ALGORITHM": algorithm,
            "SINGLE_MACHINE": 0,
            "VOLUMES_BLOCK": VOLUMES_BLOCK,
            "VOLUME_MOUNTS_BLOCK": VOLUME_MOUNTS_BLOCK,
        }
        try:
            with open(template_path, 'r') as f:
                template_str = f.read()
        except FileNotFoundError:
            print(f"[ERROR] Template file not found: {template_path}")
            return

        yaml_str = template_str
        for key, value in params.items():
            yaml_str = yaml_str.replace(f"${{{key}}}", str(value))

        yaml_path = os.path.join(yaml_dir, f"powergraph-mpijob-{algorithm}-multi.yaml")
        try:
            with open(yaml_path, "w") as f:
                f.write(yaml_str)
            kubectl = shutil.which("kubectl")
            if kubectl is None:
                raise RuntimeError("kubectl not found in PATH")
            print(f"[INFO] Submitting PowerGraph MPIJob: {algorithm} multi-machine, machines={machines}")
            subprocess.run([kubectl, "apply", "-f", yaml_path], check=True)
            subprocess.run([kubectl, "wait", "--for=condition=Succeeded", "mpijob/powergraph-mpijob", "--timeout=10m"], check=True)
            log_file = os.path.join(output_dir, f"powergraph_{algorithm}-n{machines}-p32.log")
            subprocess.run([kubectl, "logs", "job/powergraph-mpijob-launcher"], stdout=open(log_file, "w"), check=True)
            subprocess.run([kubectl, "delete", "-f", yaml_path], check=True)
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)
                pass

    print("[INFO] ✅ PowerGraph experiments completed.")

# --- Helper Functions ---
def run_command(command, working_dir="."):
    """Executes a shell command and prints its output."""
    click.echo(f"▶️  Executing in '{working_dir}': {' '.join(command)}")
    try:
        # Using shell=False and a list of args is safer
        result = subprocess.run(
            command,
            cwd=working_dir,
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
        click.echo("✅ Command executed successfully.")
        if result.stdout:
            click.echo("--- STDOUT ---")
            click.echo(result.stdout)
        if result.stderr:
            click.echo("--- STDERR ---")
            click.echo(result.stderr)
    except FileNotFoundError:
        click.secho(
            f"❌ Error: Command '{command[0]}' not found. Make sure it's in your PATH or in the '{working_dir}' directory.",
            fg="red")
    except subprocess.CalledProcessError as e:
        click.secho(f"❌ Command failed with exit code: {e.returncode}", fg="red")
        if e.stdout:
            click.secho("--- STDOUT ---", fg="yellow")
            click.secho(e.stdout, fg="yellow")
        if e.stderr:
            click.secho("--- STDERR ---", fg="yellow")
            click.secho(e.stderr, fg="yellow")
    except Exception as e:
        click.secho(f"💥 An unexpected error occurred: {e}", fg="red")


# --- Helper Functions for Performance Evaluation ---
def download_sample_datasets():
    """Download sample datasets if not already present."""
    dataset_urls = [
        "https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/sample_data.zip",
    ]
    
    # Check if sample_data folder exists; if so, skip download
    if os.path.exists("sample_data"):
        click.secho("sample_data folder already exists. Skipping dataset download.", fg="cyan")
        return
    
    for url in dataset_urls:
        filename = os.path.basename(url)
        file_path = os.path.join(filename)
        if not os.path.exists(file_path):
            click.secho(f"Downloading dataset: {filename} ...", fg="yellow")
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                click.secho(f"✅ Downloaded: {filename}", fg="green")
                if filename.endswith(".zip"):
                    with zipfile.ZipFile(file_path, "r") as zip_ref:
                        zip_ref.extractall()
                    click.secho(f"✅ Extracted: {filename}", fg="green")
                    # Remove zip file after extraction
                    os.remove(file_path)
                    click.secho(f"🗑️ Removed zip file: {filename}", fg="green")
            except Exception as e:
                click.secho(f"❌ Failed to download {filename}: {e}", fg="red")
        else:
            click.secho(f"Dataset already exists: {filename}", fg="cyan")


def setup_graphx_files(platform_dir):
    """Download GraphX-specific jar and shell script files."""
    graphx_base_url = "https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/"
    graphx_files = [
        "pagerankexample_2.11-0.1.jar",
        "ssspexample_2.11-0.1.jar",
        "trianglecountingexample_2.11-0.1.jar",
        "labelpropagationexample_2.11-0.1.jar",
        "coreexample_2.11-0.1.jar",
        "connectedcomponentexample_2.11-0.1.jar",
        "betweennesscentralityexample_2.11-0.1.jar",
        "kcliqueexample_2.11-0.1.jar",
    ]
    graphx_sh_files = [
        "pagerank.sh", "sssp.sh", "trianglecounting.sh", "labelpropagation.sh",
        "core.sh", "connectedcomponent.sh", "betweennesscentrality.sh", "kclique.sh"
    ]
    
    # If GraphX directory does not exist, create it
    if not os.path.exists(platform_dir):
        click.secho(f"GraphX directory '{platform_dir}' not found. Creating...", fg="yellow")
        os.makedirs(platform_dir, exist_ok=True)

    # Download .jar files if missing
    for fname in graphx_files:
        fpath = os.path.join(platform_dir, fname)
        url = graphx_base_url + fname
        if not os.path.exists(fpath):
            click.secho(f"Downloading GraphX jar: {fname} ...", fg="yellow")
            try:
                resp = requests.get(url, stream=True)
                resp.raise_for_status()
                with open(fpath, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                click.secho(f"✅ Downloaded: {fname}", fg="green")
            except Exception as e:
                click.secho(f"❌ Failed to download {fname}: {e}", fg="red")
        else:
            click.secho(f"Jar already exists: {fname}", fg="cyan")
            
    # Download .sh files if missing
    for sh_file in graphx_sh_files:
        sh_path = os.path.join(platform_dir, sh_file)
        url = graphx_base_url + sh_file
        if not os.path.exists(sh_path):
            click.secho(f"Downloading GraphX script: {sh_file} ...", fg="yellow")
            try:
                resp = requests.get(url, stream=True)
                resp.raise_for_status()
                with open(sh_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                click.secho(f"✅ Downloaded: {sh_file}", fg="green")
            except Exception as e:
                click.secho(f"❌ Failed to download {sh_file}: {e}", fg="red")
        else:
            click.secho(f"Script already exists: {sh_file}", fg="cyan")


# --- Platform & Algorithm Definitions ---
PLATFORM_CONFIG = {
    "flash": {"dir": "Flash", "algos": {"pagerank": "pagerank", "sssp": "sssp", "triangle": "triangle", "lpa": "lpa", "cd": "k-core-search", "kclique": "clique", "cc": "cc", "bc": "bc"}},
    "ligra": {"dir": "Ligra",
              "algos": {"pagerank": "PageRank", "sssp": "BellmanFord", "bc": "BC", "kclique": "KCLIQUE", "cd": "KCore", "lpa": "LPA", "cc": "Components", "triangle": "Triangle"}},
    "grape": {"dir": "Grape",
              "algos": {"pagerank": "pagerank", "sssp": "sssp", "bc": "bc", "kclique": "kclique", "cd": "core_decomposition", "lpa": "cdlp", "cc": "wcc", "triangle": "lcc"}},
    "pregel+": {"dir": "Pregel+", "algos": {"pagerank": "pagerank", "sssp": "sssp", "bc": "betweenness", "lpa": "lpa", "kclique": "clique", "triangle": "triangle", "cc": "cc"}},
    "gthinker": {"dir": "Gthinker", "algos": {"kclique": "clique", "triangle": "triangle"}},
    "powergraph": {"dir": "PowerGraph", "algos": {"pagerank": "pagerank", "sssp": "sssp", "triangle": "triangle", "lpa": "lpa", "cd": "kcore", "cc": "cc", "bc": "betweenness"}},
    "graphx": {"dir": "GraphX",
               "algos": {"pagerank": "pagerank", "sssp": "sssp", "triangle": "triangle", "lpa": "lpa", "cd": "cd", "cc": "cc", "bc": "bc", "kclique": "kclique"}}
}


# --- CLI Main Group ---
@click.group(context_settings=dict(help_option_names=['-h', '--help']))
def main():
    """
    Unified CLI for Graph-Analytics-Benchmarks.

    Provides a single entry point for data generation, LLM usability evaluation,
    and cross-platform performance benchmarking.
    """
    pass


# --- 1. Data Generator ---
@main.command(name="datagen", help="--scale <S> --platform <P> --feature <F>")
@click.option('--scale', required=True, type=str, help="Dataset scale (e.g., 8, 9, 10).")
@click.option('--platform', required=True, type=click.Choice(['flash', 'ligra', 'grape', 'gthinker', 'pregel+', 'powergraph', 'graphx'], case_sensitive=False),
              help="Target platform for output format.")
@click.option('--feature', required=True, type=click.Choice(['Standard', 'Density', 'Diameter'], case_sensitive=False),
              help="Dataset feature.")
def data_generator(scale, platform, feature, compile):
    """Run the FFT-DG data generator."""
    generator_dir = "Data_Generator"
    generator_exe = "./generator"

    if not os.path.exists(os.path.join(generator_dir, "FFT-DG.cpp")):
        click.secho(f"'{generator_dir}/FFT-DG.cpp' not found. Downloading Data_Generator.zip...", fg="yellow")
        url = "https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/Data_Generator.zip"
        try:
            r = requests.get(url)
            r.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                z.extractall(generator_dir)
            click.secho("✅ Data_Generator.zip downloaded and extracted.", fg="green")
        except Exception as e:
            click.secho(f"Error downloading or extracting Data_Generator.zip: {e}", fg="red")
            return

    if not os.path.exists(os.path.join(generator_dir, "generator")):
        compile_cmd = ["g++", "FFT-DG.cpp", "-o", "generator", "-O3"]
        run_command(compile_cmd, working_dir=generator_dir)

    run_cmd = [generator_exe, str(scale), platform, feature]
    run_command(run_cmd, working_dir=generator_dir)


# --- 2. LLM-based Usability Evaluation ---
@main.command(name="llm-eval", help="--platform <P> --algorithm <A>")
@click.option('--platform', help="The target platform to evaluate (e.g., 'grape').")
@click.option('--algorithm', help="The target algorithm to evaluate (e.g., 'pagerank').")
def llm_evaluation(platform, algorithm):
    """Run the LLM-based usability evaluation."""
    # Ensure .env exists for API key loading
    if not os.path.exists(".env"):
        click.secho("'.env' file not found. Copying from '.env.example'...", fg="yellow")
        shutil.copy(".env.example", ".env")

    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key or api_key == "your_openai_api_key_here":
        click.secho("❌ Error: OPENAI_API_KEY is not set in the .env file.", fg="red")
        return

    docker_image = "graphanalysisbenchmarks/llm-eval:latest"
    
    # Get docker executable path for security
    docker = shutil.which("docker")
    if docker is None:
        click.secho("❌ Error: docker command not found in PATH. Please install Docker.", fg="red")
        return
    
    # Check if the docker image exists locally, if not, try to pull it
    try:
        result = subprocess.run([docker, "image", "inspect", docker_image], capture_output=True)
        if result.returncode != 0:
            click.secho(f"'{docker_image}' image not found locally. Pulling from Docker Hub...", fg="yellow")
            pull_result = subprocess.run([docker, "pull", docker_image], check=True)
            click.secho(f"✅ '{docker_image}' image pulled successfully.", fg="green")
    except Exception as e:
        click.secho(f"Error checking or pulling Docker image '{docker_image}': {e}", fg="red")
        return

    if platform and algorithm:
        cmd = [docker, "run", "--rm", "-e", f"OPENAI_API_KEY={api_key}", "-e", f"PLATFORM={platform}", "-e",
               f"ALGORITHM={algorithm}", docker_image]
        run_command(cmd)
    else:
        click.secho("Error: Both --platform and --algorithm are required.", fg="red")


# --- 3. Performance Evaluation ---
@main.command(name="perf-eval", help="--platform <P> --algorithm <A> --path <D> --spark-master <M>")
@click.option('--platform', required=True, type=click.Choice(PLATFORM_CONFIG.keys(), case_sensitive=False),
              help="The platform to run the benchmark on.")
@click.option('--algorithm', required=True, type=str, help="The algorithm to run.")
@click.option('--path', 'data_path', required=False, default=None, type=click.Path(exists=False),
              help="Path to the dataset file.")
@click.option('--spark-master', default=None, help="Spark Master URL. Required only for GraphX platform.")
def perf(platform, algorithm, data_path, spark_master):
    """Run a performance benchmark for a specified platform and algorithm."""

    platform = platform.lower()  # Normalize platform name
    config = PLATFORM_CONFIG.get(platform)
    algos_map = config.get('algos', {})

    # Validate if the user's input is a valid standard algorithm name (a key in the map)
    if algorithm not in algos_map.keys():
        click.secho(f"❌ Error: Algorithm '{algorithm}' is not supported by platform '{platform}'.", fg="red")
        click.echo(f"Supported standard algorithms for '{platform}': {', '.join(algos_map.keys())}")
        sys.exit(1)

    # Translate the standard name to the platform-specific name for execution
    platform_specific_algorithm = algos_map[algorithm]
    platform_dir = config['dir']

    # Download sample datasets
    download_sample_datasets()

    if platform == 'graphx':
        # Setup GraphX-specific files
        setup_graphx_files(platform_dir)

        if not spark_master:
            click.secho("❌ Error: --spark-master is required for the 'graphx' platform.", fg="red")
            return

        script_name_map = {
            "pagerank": "pagerank.sh", "sssp": "sssp.sh", "triangle": "trianglecounting.sh",
            "lpa": "labelpropagation.sh", "cd": "core.sh", "cc": "connectedcomponent.sh",
            "bc": "betweennesscentrality.sh", "kclique": "kclique.sh"
        }
        # The key 'algorithm' is the standard name from user input
        script_filename = script_name_map.get(algorithm)

        if not script_filename:
            click.secho(f"Internal error: No script mapping for GraphX algorithm '{algorithm}'.", fg="red")
            return

        script_path = os.path.join(platform_dir, script_filename)

        if not os.path.exists(script_path):
            click.secho(f"❌ Error: Script '{script_path}' not found.", fg="red")
            return

        # If data_path is None, use default sample graph according to algorithm
        if data_path is None:
            if algorithm == "pagerank":
                data_path = os.path.abspath("sample_data/graphx_sample_graph.txt")
            else:
                data_path = os.path.abspath("sample_data/graphx_weight_sample_graph.txt")
        cmd = [f"./{script_filename}", spark_master, data_path]
        run_command(cmd, working_dir=platform_dir)

    # General handling for all other platforms
    else:
        
        if platform == "ligra":
            run_ligra_perf(platform_specific_algorithm, data_path)
        elif platform == "grape":
            run_grape_perf(platform_specific_algorithm, data_path)      
        elif platform == "pregel+":
            run_pregel_perf(platform_specific_algorithm, data_path)
        elif platform == "gthinker":
            run_gthinker_perf(platform_specific_algorithm, data_path)
        elif platform == "powergraph":
            run_powergraph_perf(platform_specific_algorithm, data_path)
        elif platform == "flash":
            run_flash_perf(platform_specific_algorithm, data_path)


if __name__ == "__main__":
    main()
