# master.py
from flask import Flask, jsonify, send_from_directory, request
from werkzeug.utils import secure_filename
import os, threading, time, subprocess
from streamlit_autorefresh import st_autorefresh

# ---- Flask part (master API for workers) ------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHUNKS_DIR = os.path.join(BASE_DIR, "chunks")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
SPLIT_PATH = CHUNKS_DIR+"\\input_$num%03d$.mp4"

os.makedirs(CHUNKS_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024 * 1024  # 10GB max (adjust)

jobs_lock = threading.Lock()


def build_jobs():
    files = sorted(f for f in os.listdir(CHUNKS_DIR) if f.lower().endswith(".mp4"))
    return [{"name": f, "status": "queued"} for f in files]


# this "jobs" is only used inside Flask; Streamlit will NEVER touch it directly
jobs = build_jobs()


@app.route("/get_job", methods=["GET"])
def get_job():
    worker_id = request.args.get("worker_id", "unknown")
    print("worker connected:", worker_id)

    with jobs_lock:
        for job in jobs:
            if job["status"] == "queued":
                job["status"] = "processing"
                job["worker"] = worker_id
                job["start_ts"] = time.time()
                return jsonify(
                    {
                        "chunk": job["name"],
                        "url": f"http://{request.host}/chunks/{job['name']}",
                    }
                )
    return jsonify({"chunk": None})


@app.route("/report_result", methods=["POST"])
def report_result():
    data = request.get_json() or {}
    name = data.get("chunk")
    ok = False
    with jobs_lock:
        for job in jobs:
            if job["name"] == name:
                job["status"] = "done"
                job["end_ts"] = time.time()
                ok = True
                break
    return jsonify({"ok": ok})


@app.route("/upload_result", methods=["POST"])
def upload_result():
    chunk_name = request.form.get("chunk")
    file = request.files.get("file")
    if not file or not chunk_name:
        return jsonify({"ok": False, "message": "missing chunk or file"}), 400

    filename = secure_filename(file.filename)
    save_path = os.path.join(RESULTS_DIR, filename)
    file.save(save_path)

    with jobs_lock:
        for job in jobs:
            if job["name"] == chunk_name:
                job["status"] = "done"
                job["end_ts"] = time.time()
                job["result"] = filename
                break

    print(f"Received {filename} for chunk {chunk_name}")
    return jsonify({"ok": True})


@app.route("/chunks/<path:filename>")
def serve_chunk(filename):
    return send_from_directory(CHUNKS_DIR, filename, as_attachment=True)


@app.route("/status", methods=["GET"])
def status():
    with jobs_lock:
        return jsonify(jobs)


@app.route("/reload_jobs", methods=["POST"])
def reload_jobs():
    global jobs
    with jobs_lock:
        jobs = build_jobs()
        print(f"jobs reloaded: {len(jobs)}")
        return jsonify({"count": len(jobs)})


# ---- Streamlit UI part ------------------------------------------------------
import streamlit as st
import requests
import pandas as pd


MASTER_BASE = "http://127.0.0.1:5000"  # UI talks to Flask via localhost


def reset_directories():
    for folder in (CHUNKS_DIR, RESULTS_DIR):
        for f in os.listdir(folder):
            try:
                os.remove(os.path.join(folder, f))
            except OSError:
                pass


def split_video_locally(input_path: str, chunk_seconds: int = 60):
    """
    Split the input video using MP4Box into CHUNKS_DIR.
    Does NOT touch Flask jobs directly; we call /reload_jobs after this.
    """
    reset_directories()

    # MP4Box -split-time <seconds> <input>
    # Run inside CHUNKS_DIR so outputs are created there
    cmd = ["MP4Box", "-split", str(chunk_seconds), input_path,"-out",SPLIT_PATH]
    subprocess.run(cmd, check=True, cwd=CHUNKS_DIR)


def fetch_jobs_from_api():
    resp = requests.get(MASTER_BASE + "/status", timeout=5)
    resp.raise_for_status()
    return resp.json()


def all_jobs_done_from_api():
    js = fetch_jobs_from_api()
    if not js:
        return False
    return all(j.get("status") == "done" for j in js)


def merge_results(output_name="final_merged.mp4"):
    """
    Merge processed chunks in RESULTS_DIR using ffmpeg concat.
    Job order is taken from /status (Flask).
    """
    jobs_status = fetch_jobs_from_api()
    ordered_jobs = sorted(jobs_status, key=lambda j: j["name"])

    input_paths = []
    for j in ordered_jobs:
        result_file = j.get("result")
        if not result_file:
            continue
        full_path = os.path.join(RESULTS_DIR, result_file)
        if os.path.exists(full_path):
            input_paths.append(full_path)

    if not input_paths:
        raise RuntimeError("No processed chunks found to merge.")

    files_txt = os.path.join(BASE_DIR, "files.txt")
    with open(files_txt, "w", encoding="utf-8") as f:
        for path in input_paths:
            f.write(f"file '{path}'\n")

    out_path = os.path.join(BASE_DIR, output_name)

    # try stream copy
    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        files_txt,
        "-c",
        "copy",
        out_path,
    ]
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        # fallback re-encode
        cmd = [
            "ffmpeg",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            files_txt,
            "-c:v",
            "libx264",
            "-preset",
            "medium",
            "-crf",
            "24",
            "-c:a",
            "aac",
            out_path,
        ]
        subprocess.run(cmd, check=True)

    return out_path


def streamlit_ui():
    st.title("Distributed Video Encoder (Master)")

    st.markdown(
        "Upload a large MP4, split it into chunks, let workers encode them via Flask API, "
        "and automatically merge the results into a single video when finished."
    )

    # ensure key exists
    if "merged_path" not in st.session_state:
        st.session_state["merged_path"] = None

    with st.sidebar:
        st.header("Upload & Settings")
        chunk_len = st.number_input(
            "Chunk length (seconds)", min_value=10, max_value=900, value=60, step=10
        )
        uploaded = st.file_uploader("Upload MP4 video", type=["mp4"])

        if st.button("Start processing"):
            if uploaded is None:
                st.error("Please upload a video first.")
            else:
                # new run => clear old merge result
                st.session_state["merged_path"] = None

                input_path = os.path.join(BASE_DIR, "input.mp4")
                with open(input_path, "wb") as f:
                    f.write(uploaded.getbuffer())

                try:
                    split_video_locally(input_path, int(chunk_len))
                    # tell Flask to rebuild jobs from new chunk files
                    r = requests.post(MASTER_BASE + "/reload_jobs", timeout=10)
                    r.raise_for_status()
                    count = r.json().get("count", 0)
                    st.success(f"Video split into {count} chunks. Workers can start now.")
                except Exception as e:
                    st.error(f"Error during splitting/reloading: {e}")

    st.subheader("Job Status")

    try:
        js = fetch_jobs_from_api()
    except Exception as e:
        st.error(f"Could not fetch status from Flask API: {e}")
        js = []

    has_jobs = bool(js)
    all_done = has_jobs and all(j.get("status") == "done" for j in js)

    # Auto-refresh while work is in progress (some jobs not done yet)
    if has_jobs and not all_done:
        st_autorefresh(interval=3000, limit=None, key="job_refresh")

    if js:
        df = pd.DataFrame(js)
        st.dataframe(df)
    else:
        st.info("No jobs yet. Upload a video and click 'Start processing'.")

    # When all jobs complete, auto-merge once
    if has_jobs and all_done:
        if st.session_state["merged_path"] is None:
            with st.spinner("All chunks done. Merging processed chunks..."):
                try:
                    out_path = merge_results()
                    st.session_state["merged_path"] = out_path
                    st.success("Merging completed.")
                except Exception as e:
                    st.error(f"Error during merge: {e}")
                    return

        # show download button for already-merged file
        out_path = st.session_state["merged_path"]
        if out_path and os.path.exists(out_path):
            st.success("All chunks processed and merged successfully.")
            with open(out_path, "rb") as f:
                st.download_button(
                    label="Download merged video",
                    data=f,
                    file_name=os.path.basename(out_path),
                    mime="video/mp4",
                )
            for folder in (CHUNKS_DIR, RESULTS_DIR):
                for f in os.listdir(folder):
                    try:
                        os.remove(os.path.join(folder, f))
                    except OSError:
                        pass
        else:
            st.error("Merged file not found on disk.")
    else:
        st.info("Waiting for workers to finish processing chunks...")



def run_flask():
    # 0.0.0.0 so other machines on LAN can reach it
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)


# start Flask only once
if "flask_thread_started" not in st.session_state:
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()
    st.session_state["flask_thread_started"] = True

# run the UI
streamlit_ui()
