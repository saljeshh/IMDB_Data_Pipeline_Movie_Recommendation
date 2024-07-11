from connection_str import engine
import pandas as pd
from datetime import datetime



def log_etl(run_id, stage, status, count_rows=None, error_msg=None, start=None, end=None):
    
    duration = (end - start).total_seconds() if start and end else None
    
    log_df = pd.DataFrame({
        'run_id': [run_id],
        'stage': [stage],
        'status': [status],
        'count_rows': [count_rows],
        'error_msg': [error_msg],
        'timestamp': [datetime.now()],
        'duration_seconds': [duration]
    })
    log_df.to_sql('etl_log', con=engine, if_exists='append', index=False)