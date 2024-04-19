import logging
import os

def setup_logging():
    logging.basicConfig(filename=r'C:\Users\john.dees\PycharmProjects\Building-ETL-Pipelines-with-Python\logs\etl_pipeline_main.log',
                        # filename=os.path.join('logs', 'etl_pipeline_main.log'),
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')