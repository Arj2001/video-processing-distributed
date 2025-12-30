# 🎥 Video Processing Distributed

## 🧠 Master Controller

This project implements a **distributed video processing system** using a master–worker model.
You control everything from a simple UI, while worker nodes handle video chunks in parallel.

### 🔧 Tech Stack

* 🖥️ Streamlit for the master UI
* 🌐 Flask for worker communication
* 🎞️ FFmpeg for encoding and merging
* ✂️ MP4Box for video splitting
* 🧩 Multiple worker nodes for parallel processing

---

## ⚙️ How the System Works

The master node performs the following steps automatically:

1. 📥 Accepts a large input MP4 file
2. ✂️ Splits the video into time-based chunks
3. 📤 Assigns chunks to available worker nodes
4. ⏳ Waits for all workers to finish processing
5. 🔗 Merges processed chunks into a final MP4
6. 🧹 Cleans up all temporary chunk and output folders

---

## 📦 Requirements

### 🐍 Python Libraries (Master Node)

Install using pip:

* flask
* werkzeug
* streamlit
* streamlit-autorefresh
* pandas
* requests

### 🧑‍💻 Worker Node Requirements

Only the following are required:

* requests
* ffmpeg

---

## 🛠️ System Dependencies

### ✂️ MP4Box (Master Node Only)

Used for splitting videos into chunks.

* Command used:
  MP4Box -split `<seconds>`
* Download via official GPAC site or YouTube guides
* Ensure MP4Box is added to system PATH

### 🎞️ FFmpeg (Master + Worker Nodes)

Used for encoding and merging video chunks.

Install on Windows:

```
winget install ffmpeg
```

Verify both `ffmpeg` and `MP4Box` are accessible from the terminal.

---

## ▶️ Running the Master Node

Start the master controller UI:

```
streamlit run master.py
```

---

## 🔐 Network Configuration

* Master node listens on port **5000**
* Ensure inbound TCP traffic is allowed on this port

### Windows Firewall Setup

* Add a new inbound rule
* Protocol: TCP
* Port: 5000
* Action: Allow

---

## 🚀 Use Case

This project demonstrates:

* Distributed computing fundamentals
* Parallel video encoding
* Master–worker architecture
* Practical use of FFmpeg in Python systems

Ideal for academic experiments, demos, and learning distributed systems through multimedia processing.
