import prefect
from prefect import task, Flow, Parameter

from tiled.client import from_profile

tiled_client = from_profile("nsls2", username=None)["cms"]
tiled_client_raw = tiled_client["raw"]
# This needs the sandbox in place first
# tiled_client_processed = tiled_client["sandbox"]


@task
def analysis(ref):
    logger = prefect.context.get("logger")
    logger.info("Analysis starting...")
    run = tiled_client_raw[ref]
    # Get the full uid for ease of finding analyzed results
    # written to analysis databroker later
    full_uid = run.start["uid"]
    logger.info(f"Full uid = {full_uid}")
    # Do any data processing/calling other functions for data
    # processing here
    logger.info("Analysis complete")

with Flow("analysis") as flow:
    raw_ref = Parameter("ref")
    analysis(raw_ref)
