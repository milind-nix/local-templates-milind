from datetime import datetime, timezone

from prefect import flow, get_run_logger, task


@task(name="build-smoke-test-result")
def build_smoke_test_result(workspace_id: int, workflow_id: int, message: str) -> dict:
    return {
        "workspace_id": workspace_id,
        "workflow_id": workflow_id,
        "message": message,
        "ran_at": datetime.now(timezone.utc).isoformat(),
    }


@flow(name="nextier-workflow-smoke-test")
def nextier_workflow_smoke_test_flow(
    workspace_id: int,
    workflow_id: int,
    message: str = "nextier-test workflow smoke test",
):
    logger = get_run_logger()
    result = build_smoke_test_result(
        workspace_id=workspace_id,
        workflow_id=workflow_id,
        message=message,
    )
    logger.info("Nextier workflow smoke test result: %s", result)
    return result
