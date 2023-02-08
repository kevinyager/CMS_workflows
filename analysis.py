import numpy as np
import prefect
from prefect import task, Flow, Parameter
import sys
from tiled.client import from_profile

tiled_client = from_profile("nsls2", username=None)["cms"]
tiled_client_raw = tiled_client["raw"]
tiled_client_processed = tiled_client["sandbox"]


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
    primary_data = run["primary"]["data"].read()

    # Grab some data to test writing to tiled sandbox
    data = primary_data["pilatus2M_image"][0, 0, :5, :5]
    # Include the raw uid from the original scan the processed
    # data comes from to search for later
    md = {"python_environment": sys.prefix,
          "raw_uid": full_uid}
    # Now write the data
    tiled_client_processed.write_array(
            np.array(data),
            metadata=md
    )
    logger.info("Analysis complete")

with Flow("analysis") as flow:
    raw_ref = Parameter("ref")
    analysis(raw_ref)
