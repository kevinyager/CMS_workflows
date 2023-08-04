from prefect import flow, get_run_logger, task

from analysis import analysis_flow
from data_validation import general_data_validation


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow
def end_of_run_workflow(stop_doc):
    uid = stop_doc["run_start"]

    general_data_validation(beamline_acronym="cms", uid=uid)

    # Comment out the next line if you want to disable (avoid creating workflows):
    analysis_flow(raw_ref=uid)
