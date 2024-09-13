import os
import time
import subprocess
from datetime import datetime
import logging
import argparse

def execute_script(script_path):
    """Execute a single script and log its status."""
    start_time = time.time()
    try:
        logging.info(f"Executing: {script_path}")
        result = subprocess.run(['python', script_path], check=True)
        execution_time = time.time() - start_time
        logging.info(f"Completed: {script_path} in {execution_time:.2f} seconds")
    except subprocess.CalledProcessError as e:
        logging.error(f"Script {script_path} failed with error code {e.returncode}")
    except Exception as e:
        logging.exception(f"Error executing script {script_path}: {e}")

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Execute multiple scripts sequentially.')
    parser.add_argument('--log-file', default='scripts_task.log', help='Path to the log file.')
    parser.add_argument('--base-path', default='c:/Users/Administrator/OneDrive - CARBON CARS/PowerBI/Scripts/carbon-bi/', help='Base path for scripts.')
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        handlers=[
            logging.FileHandler(args.log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    # List of scripts to execute
    scripts = [
        os.path.join(args.base_path, 'ph_paid_users_local.py'),
        os.path.join(args.base_path, 'ph_overview_local.py'),
        os.path.join(args.base_path, 'ph_rd_lp_pageviews_local.py'),
        os.path.join(args.base_path, 'ph_rd_events_local.py'),
        os.path.join(args.base_path, 'rd_station_SDR_deals_local.py'),
        os.path.join(args.base_path, 'rd_station_BDR_deals_local.py'),
        os.path.join(args.base_path, 'trello_local.py')
    ]

    total_start_time = time.time()
    logging.info("### START ###")
    logging.info("----------------------------------------")

    try:
        # Execute scripts sequentially
        for script_path in scripts:
            execute_script(script_path)
    except KeyboardInterrupt:
        logging.warning("Script execution interrupted by the user.")
    except Exception as e:
        logging.exception(f"An unexpected error occurred: {e}")
    finally:
        total_execution_time = time.time() - total_start_time
        end_time = datetime.now().strftime("%H:%M:%S")
        logging.info("----------------------------------------")
        logging.info(f"Total execution time: {total_execution_time:.2f} seconds")
        logging.info(f"End time: {end_time}")
        logging.info("### END ###")

if __name__ == "__main__":
    main()
