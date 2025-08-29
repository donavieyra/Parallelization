import os
import socket
import threading
import sqlite3
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from common import send_msg, recv_msg

load_dotenv()
from dotenv import load_dotenv
import os

load_dotenv("config.env")  # explicitly load your config.env
CSV_PATH = os.getenv("CSV_PATH")
print(f"[DEBUG] CSV_PATH = {CSV_PATH}")  # optional debug


CSV_PATH = os.getenv("CSV_PATH")
SERVER_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.getenv("SERVER_PORT", 5055))
DB_PATH = os.getenv("DB_URL", "sales_results.db").replace("sqlite:///", "")
CHUNKS = int(os.getenv("CHUNKS", 8))

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()
cursor.executescript(open("schema.sql").read())
conn.commit()

print("[SERVER] Loading dataset...")
df = pd.read_csv(CSV_PATH)
chunks = np.array_split(df, CHUNKS)
del df
print(f"[SERVER] Split into {CHUNKS} chunks")

chunk_queue = list(enumerate(chunks))
lock = threading.Lock()

def handle_worker(conn_sock, addr):
    print(f"[SERVER] Worker connected: {addr}")
    while True:
        with lock:
            if not chunk_queue:
                break
            chunk_id, chunk_df = chunk_queue.pop(0)
        send_msg(conn_sock, chunk_df)
        result = recv_msg(conn_sock)
        if result:
            save_result(result, chunk_id)
    conn_sock.close()
    print(f"[SERVER] Worker {addr} finished")

def save_result(result, chunk_id):
    cursor.execute("""
    INSERT OR REPLACE INTO partial_results
    (worker_id, chunk_id, rows_processed, total_sales,
     price_min, price_max, price_sum)
    VALUES (?, ?, ?, ?, ?, ?, ?)""",
    (result["worker_id"], chunk_id, result["rows"], result["total_sales"],
     result["price_min"], result["price_max"], result["price_sum"]))
    conn.commit()
    print(f"[SERVER] Saved result from {result['worker_id']} (chunk {chunk_id})")

def aggregate_results():
    print("\n[SERVER] Final Aggregation Results")
    q = """
    SELECT SUM(rows_processed), SUM(total_sales),
           MIN(price_min), MAX(price_max),
           SUM(price_sum)*1.0/SUM(rows_processed)
    FROM partial_results;
    """
    res = cursor.execute(q).fetchone()
    print(f"Total Rows: {res[0]}")
    print(f"Total Sales: {res[1]}")
    print(f"Min Price: {res[2]} | Max Price: {res[3]} | Avg Price: {res[4]}")

def start_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((SERVER_HOST, SERVER_PORT))
    sock.listen(5)
    print(f"[SERVER] Listening on {SERVER_HOST}:{SERVER_PORT}")

    threads = []
    while True:
        try:
            conn_sock, addr = sock.accept()
            t = threading.Thread(target=handle_worker, args=(conn_sock, addr))
            t.start()
            threads.append(t)
        except KeyboardInterrupt:
            break

    for t in threads:
        t.join()
    aggregate_results()

if __name__ == "__main__":
    start_server()
