# worker.py
import requests, time, os, subprocess, sys
from urllib.parse import urljoin

MASTER = "http://192.168.220.10:5000"   # change to http://<MASTER_IP>:5000 if master on LAN
WORK_DIR = os.path.join(os.path.dirname(_file_), "worker_tmp")
os.makedirs(WORK_DIR, exist_ok=True)

WORKER_ID = f"win-laptop-{os.getpid()}"

POLL_INTERVAL = 5

def download_file(url, local_path):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)

def transcode(infile, outfile):
    # change ffmpeg params here if desired
    cmd = [
        "ffmpeg", "-y", "-i", infile,
        "-vf", "scale=-2:720,fps=24",
        "-c:v", "libx264", "-preset", "medium", "-crf", "24",
        "-c:a", "aac", "-b:a", "96k",
        outfile
    ]
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)

def upload_result(chunk_name, file_path):
    files = {"file": open(file_path, "rb")}
    data = {"chunk": chunk_name}
    resp = requests.post(MASTER + "/upload_result", files=files, data=data)
    resp.raise_for_status()
    return resp.json()

def poll_loop():
    while True:
        try:
            resp = requests.get(MASTER + "/get_job", params={"worker_id": WORKER_ID}, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            chunk = data.get("chunk")
            if not chunk:
                print("No job available — sleeping...")
                time.sleep(POLL_INTERVAL)
                continue
            url = data["url"]
            local_in = os.path.join(WORK_DIR, chunk)
            print("Downloading", url, "->", local_in)
            download_file(url, local_in)
            out_name = os.path.splitext(chunk)[0] + ".processed.mp4"
            local_out = os.path.join(WORK_DIR, out_name)
            start = time.time()
            transcode(local_in, local_out)
            end = time.time()
            print(f"Transcode done in {end-start:.1f}s. Uploading {local_out}")
            upload_result(chunk, local_out)
            # Optionally notify master via /report_result (not necessary since upload marks done)
            requests.post(MASTER + "/report_result", json={"chunk": chunk})
            # cleanup local files
            os.remove(local_in)
            os.remove(local_out)
        except Exception as e:
            print("Worker error:", e)
            time.sleep(5)

if _name_ == "_main_":
    print("Worker starting, MASTER:", MASTER, "WORKER_ID:", WORKER_ID)
    poll_loop()