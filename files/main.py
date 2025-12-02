from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import FileResponse, StreamingResponse
import os
import aiofiles
from datetime import datetime
from pathlib import Path
import json

app = FastAPI()

files_dir = os.getenv("FILES_DIR", "/app/storage")
Path(files_dir).mkdir(parents=True, exist_ok=True)
Path(os.path.join(files_dir, "logs")).mkdir(parents=True, exist_ok=True)
Path(os.path.join(files_dir, "graphs")).mkdir(parents=True, exist_ok=True)

@app.get("/files/list")
async def list_files(file_type: str = None):
    try:
        result = []
        
        if file_type == "logs" or file_type is None:
            logs_path = os.path.join(files_dir, "logs")
            if os.path.exists(logs_path):
                for filename in os.listdir(logs_path):
                    filepath = os.path.join(logs_path, filename)
                    if os.path.isfile(filepath):
                        stat = os.stat(filepath)
                        result.append({
                            "type": "log",
                            "filename": filename,
                            "size": stat.st_size,
                            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                        })
        
        if file_type == "graphs" or file_type is None:
            graphs_path = os.path.join(files_dir, "graphs")
            if os.path.exists(graphs_path):
                for filename in os.listdir(graphs_path):
                    filepath = os.path.join(graphs_path, filename)
                    if os.path.isfile(filepath):
                        stat = os.stat(filepath)
                        result.append({
                            "type": "graph",
                            "filename": filename,
                            "size": stat.st_size,
                            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                        })
        
        return {"files": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/files/upload/log")
async def upload_log(file: UploadFile = File(...)):
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="no filename")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_filename = f"{timestamp}_{file.filename}"
        filepath = os.path.join(files_dir, "logs", safe_filename)
        
        async with aiofiles.open(filepath, 'wb') as f:
            content = await file.read()
            await f.write(content)
        
        return {"status": "ok", "filename": safe_filename, "path": f"logs/{safe_filename}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/files/upload/graph")
async def upload_graph(file: UploadFile = File(...)):
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="no filename")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_filename = f"{timestamp}_{file.filename}"
        filepath = os.path.join(files_dir, "graphs", safe_filename)
        
        async with aiofiles.open(filepath, 'wb') as f:
            content = await file.read()
            await f.write(content)
        
        return {"status": "ok", "filename": safe_filename, "path": f"graphs/{safe_filename}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files/download/{file_type}/{filename}")
async def download_file(file_type: str, filename: str):
    try:
        if file_type not in ["logs", "graphs"]:
            raise HTTPException(status_code=400, detail="invalid file type")
        
        filepath = os.path.join(files_dir, file_type, filename)
        
        if not os.path.exists(filepath):
            raise HTTPException(status_code=404, detail="file not found")
        
        if not os.path.isfile(filepath):
            raise HTTPException(status_code=400, detail="not a file")
        
        return FileResponse(
            filepath,
            media_type="application/octet-stream",
            filename=filename
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files/log/{filename}")
async def get_log(filename: str):
    try:
        filepath = os.path.join(files_dir, "logs", filename)
        
        if not os.path.exists(filepath):
            raise HTTPException(status_code=404, detail="file not found")
        
        async def generate():
            async with aiofiles.open(filepath, 'r') as f:
                async for line in f:
                    yield line
        
        return StreamingResponse(generate(), media_type="text/plain")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/files/{file_type}/{filename}")
async def delete_file(file_type: str, filename: str):
    try:
        if file_type not in ["logs", "graphs"]:
            raise HTTPException(status_code=400, detail="invalid file type")
        
        filepath = os.path.join(files_dir, file_type, filename)
        
        if not os.path.exists(filepath):
            raise HTTPException(status_code=404, detail="file not found")
        
        os.remove(filepath)
        
        return {"status": "ok", "message": "file deleted"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files/stats")
async def get_stats():
    try:
        logs_path = os.path.join(files_dir, "logs")
        graphs_path = os.path.join(files_dir, "graphs")
        
        logs_count = 0
        logs_size = 0
        if os.path.exists(logs_path):
            for filename in os.listdir(logs_path):
                filepath = os.path.join(logs_path, filename)
                if os.path.isfile(filepath):
                    logs_count += 1
                    logs_size += os.path.getsize(filepath)
        
        graphs_count = 0
        graphs_size = 0
        if os.path.exists(graphs_path):
            for filename in os.listdir(graphs_path):
                filepath = os.path.join(graphs_path, filename)
                if os.path.isfile(filepath):
                    graphs_count += 1
                    graphs_size += os.path.getsize(filepath)
        
        return {
            "logs": {
                "count": logs_count,
                "total_size_bytes": logs_size
            },
            "graphs": {
                "count": graphs_count,
                "total_size_bytes": graphs_size
            },
            "total_size_bytes": logs_size + graphs_size
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    return {"status": "ok"}

