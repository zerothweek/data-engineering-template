import os
import yaml
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from sqlalchemy import create_engine, URL, text
from tqdm import tqdm
from typing import Dict, Any, Generator

# --- Logging Setup ---
logger = logging.getLogger("EL_Engine")

class DataPipelineEngine:
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
            
        with open(self.config_path, 'r') as f:
            self.config = yaml.safe_load(f)
            
        self.engines_cache = {}

    def get_connection_engine(self, connection_name: str):
        """Lazy-loads DB engine with secure env var lookup."""
        if connection_name in self.engines_cache:
            return self.engines_cache[connection_name]

        conn_config = self.config['connections'].get(connection_name)
        if not conn_config:
            raise ValueError(f"Connection '{connection_name}' undefined.")

        # Security check
        env_var = conn_config.get('password_env_var', '').strip()
        password = os.getenv(env_var)
        if not password:
            raise PermissionError(f"Missing Env Var: {env_var}")

        url = URL.create(
            drivername=conn_config['drivername'],
            username=conn_config['username'],
            password=password,
            host=conn_config['host'],
            port=conn_config['port'],
            database=conn_config['database']
        )
        
        engine = create_engine(url)
        self.engines_cache[connection_name] = engine
        return engine

    def get_total_rows(self, engine, query: str) -> int:
        """
        Runs a COUNT(*) to estimate progress bar size.
        """
        count_query = f"SELECT COUNT(*) FROM ({query}) as subquery"
        try:
            with engine.connect() as conn:
                result = conn.execute(text(count_query)).scalar()
            return result
        except Exception as e:
            logger.warning(f"Could not determine total row count: {e}")
            return 0

    def stream_data(self, engine, query: str, chunk_size: int) -> Generator[pd.DataFrame, None, None]:
        """Yields DataFrame chunks to keep memory usage low."""
        # execution_options={"stream_results": True} is critical for SQLAlchemy to not 
        # load everything into RAM before giving it to Pandas
        with engine.connect().execution_options(stream_results=True) as conn:
            for chunk in pd.read_sql(text(query), conn, chunksize=chunk_size):
                yield chunk

    def write_parquet_stream(self, data_stream, output_path: Path, compression: str = 'snappy', total_rows: int = 0):
        """Writes chunks to a single Parquet file efficiently."""
        writer = None
        
        pbar = tqdm(total=total_rows, unit="rows", desc="Extracting (Parquet)")
        
        for df_chunk in data_stream:
            # Convert Pandas chunk to Arrow Table
            # OPTIMIZATION: preserve_index=False prevents writing the useless 
            # 0,1,2... pandas index to the file, saving space.
            table = pa.Table.from_pandas(df_chunk, preserve_index=False)
            
            # Initialize Writer on first chunk (once we know the schema)
            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema, compression=compression)
            
            # Check schema consistency
            if not table.schema.equals(writer.schema):
                pass 

            writer.write_table(table)
            pbar.update(len(df_chunk))
            
        if writer:
            writer.close()
        pbar.close()

    def write_csv_stream(self, data_stream, output_path: Path, compression: str = None, total_rows: int = 0):
        """Writes chunks to a CSV file (appending). Defaults to NO compression."""
        pbar = tqdm(total=total_rows, unit="rows", desc="Extracting (CSV)")
        
        mode = 'w'
        header = True
        
        for i, df_chunk in enumerate(data_stream):
            if i > 0:
                mode = 'a'
                header = False
                
            df_chunk.to_csv(output_path, mode=mode, header=header, index=False, compression=compression)
            pbar.update(len(df_chunk))
            
        pbar.close()