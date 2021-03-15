import logging 
from jobs.data_stitch_etl import job 

if __name__ == '__main__':
    logger = logging.Logger(name="test")
    logger.setLevel('DEBUG')
    job.execute()

