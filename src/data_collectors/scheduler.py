import schedule
import time 
import data_pipeline
import logging

def run_pipeline():
    
    try:
        pipeline = data_pipeline.WildfireDataPipeline()
        pipeline.run_pipeline()
        logging.info("Pipeline completed successfully")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")


schedule.every(4).hours.do(run_pipeline)

while True:
    schedule.run_pending() #checks if scheduler has a pending pipeline deadline to run
    time.sleep(60) #relaxing on cpu using and reloading to check for schedule pending times