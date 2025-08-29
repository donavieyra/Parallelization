import socket
import argparse
import pandas as pd
from common import send_msg, recv_msg

def compute_metrics(df, worker_id):
    df["Total"] = df["Price"] * df["Quantity"]
    return {
        "worker_id": worker_id,
        "rows": len(df),
        "total_sales": df["Total"].sum(),
        "price_min": df["Price"].min(),
        "price_max": df["Price"].max(),
        "price_sum": df["Price"].sum()
    }

def run_worker(worker_id, server_host, server_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((server_host, server_port))
    print(f"[WORKER-{worker_id}] Connected to server")

    while True:
        df = recv_msg(sock)
        if df is None:
            break
        result = compute_metrics(df, worker_id)
        send_msg(sock, result)
    sock.close()
    print(f"[WORKER-{worker_id}] Finished")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--server", default="127.0.0.1:5055")
    args = parser.parse_args()

    host, port = args.server.split(":")
    run_worker(args.worker_id, host, int(port))
