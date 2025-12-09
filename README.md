# video-processing-distributed

## Master Controller

This project provides a **distributed video processing system** using:
- **Streamlit** for the UI  
- **Flask** for the worker-facing API  
- **FFmpeg** for merging  
- **MP4Box** for video splitting  
- **Multiple worker nodes** that pick up chunks, encode them, and return processed video parts.

The master automatically:
1. Accepts a large input MP4
2. Splits it into chunks
3. Assigns each chunk to workers
4. Waits for all jobs to complete
5. Merges encoded chunks into a final MP4
6. Deletes all temporary chunk folders (chunks + results)

---

## Requirements

### Python libraries (install with pip)
- flask
- werkzeug
- streamlit
- streamlit-autorefresh
- pandas
- requests
for worker only need to install only `requests`

### System Dependencies (must be installed manually)
- **MP4Box (GPAC)-(Only needed in master node)**  
  Required for splitting video into chunks: MP4Box -split <seconds>
  Refer google or youtube for downloading it
- **FFmpeg(Needed both in master and worker node)**  
  Required for merging processed chunks.
  Install in windows using
  ```
  winget ffmpeg
  ```
Ensure both `ffmpeg` and `MP4Box` are available in your system PATH.

Run the master node using streamlit
```
streamlit run master.py
```
Ensure that the system is configured for getting inbound through the specified port. Used here is **5000**.
You can set up in windows by adding a new inbound tcp for port 5000 rule in firewall.




