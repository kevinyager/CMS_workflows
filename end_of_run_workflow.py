import prefect
from prefect import task, Flow, Parameter
from prefect.tasks.prefect import create_flow_run


@task
def log_completion():
    logger = prefect.context.get("logger")
    logger.info("Complete")


with Flow("end-of-run-workflow") as flow:
    stop_doc = Parameter("stop_doc")
    uid = stop_doc["run_start"]
    upstream_tasks = []

    validation_flow = create_flow_run(
        flow_name="general-data-validation",
        project_name="CMS",
        parameters={"beamline_acronym": "cms", "uid": uid}
    )
    upstream_tasks.append(validation_flow)

    analysis_flow = create_flow_run(
        flow_name="analysis",
        project_name="CMS",
        parameters={"ref": uid}
    )
    upstream_tasks.append(analysis_flow)

    # upstream_tasks must all run before log_completion can run
    log_completion(upstream_tasks=upstream_tasks)
