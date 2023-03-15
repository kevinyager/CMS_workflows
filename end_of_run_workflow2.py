from analysis2 import analysis_flow
from data_validation2 import general_data_validation
from prefect import flow, task, get_run_logger


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")

@flow
def end_of_run_workflow(stop_doc):
    uid = stop_doc["run_start"]

    general_data_validation(beamline_acronym="cms", uid=uid)

    analysis_flow(raw_ref=uid)
