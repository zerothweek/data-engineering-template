import sys
import os
import json
import time
import shutil
import logging
import getpass
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
from el_lib import DataPipelineEngine

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("pipeline_execution.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def get_git_hash():
    try:
        return subprocess.check_output(['git', 'rev-parse', 'HEAD'], stderr=subprocess.DEVNULL).decode('ascii').strip()
    except:
        return "unknown"

def main():
    parser = argparse.ArgumentParser(description="Enterprise Big Data ELT Pipeline")
    parser.add_argument("job_name", help="Job name from config")
    parser.add_argument("--version", type=str, default=None, help="Custom version label (e.g. v1.0)")
    parser.add_argument("--annotation", type=str, default="", help="Run description")
    parser.add_argument("--config", type=str, default="pipeline_config.yaml", help="Config file path")
    parser.add_argument("--format", type=str, choices=['parquet', 'csv'], help="Override output format")
    
    args = parser.parse_args()
    
    # 1. Initialize Engine
    try:
        engine_lib = DataPipelineEngine(args.config)
    except Exception as e:
        logger.error(f"Initialization Failed: {e}")
        sys.exit(1)

    # 2. Load Job Config
    job_config = engine_lib.config['jobs'].get(args.job_name)
    if not job_config:
        logger.error(f"Job '{args.job_name}' not found.")
        sys.exit(1)

    start_time = time.time()
    
    # 3. Setup Paths & Format
    version_id = args.version if args.version else datetime.now().strftime("%Y%m%d_%H%M%S")
    base_path = Path(engine_lib.config['storage']['base_path'])
    output_dir = base_path / args.job_name / version_id
    
    # Safety check
    if output_dir.exists() and args.version:
        logger.error(f"Version {version_id} already exists.")
        sys.exit(1)
        
    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine Format
    output_fmt = args.format if args.format else engine_lib.config['storage'].get('default_format', 'parquet')
    
    # Configure compression based on format
    if output_fmt == 'parquet':
        # Use config compression for Parquet (default snappy)
        compression = engine_lib.config['storage'].get('default_compression', 'snappy')
        file_ext = "parquet"
    else:
        # Force plain CSV (no compression) as requested
        compression = None
        file_ext = "csv"

    output_file = output_dir / f"data.{file_ext}"

    logger.info(f"Starting Job: {args.job_name} -> {output_file}")

    # 4. EXECUTION
    try:
        db_engine = engine_lib.get_connection_engine(job_config['connection'])
        query = job_config['query']
        chunk_size = engine_lib.config.get('execution', {}).get('chunk_size', 50000)

        # A. Get Count (for progress bar)
        logger.info("Calculating total rows for progress bar...")
        total_rows = engine_lib.get_total_rows(db_engine, query)
        logger.info(f"Total Rows detected: {total_rows:,}")

        # B. Start Stream
        stream = engine_lib.stream_data(db_engine, query, chunk_size)

        # C. Write Stream
        if output_fmt == 'parquet':
            engine_lib.write_parquet_stream(stream, output_file, compression, total_rows)
        else:
            engine_lib.write_csv_stream(stream, output_file, compression, total_rows)

        # 5. Metadata & Cleanup
        shutil.copy(args.config, output_dir / "snapshot_config.yaml")
        
        duration = time.time() - start_time
        meta = {
            "job": args.job_name,
            "version": version_id,
            "format": output_fmt,
            "rows": total_rows,
            "duration_sec": round(duration, 2),
            "git_hash": get_git_hash(),
            "annotation": args.annotation,
            "command": f"python {' '.join(sys.argv)}"
        }
        
        with open(output_dir / "metadata.json", "w") as f:
            json.dump(meta, f, indent=4)
            
        logger.info(f"DONE. Duration: {duration:.2f}s")

    except Exception as e:
        logger.error(f"Pipeline Failed: {e}", exc_info=True)
        # Clean up partial files if failed
        if output_file.exists():
            os.remove(output_file)
        sys.exit(1)

if __name__ == "__main__":
    main()