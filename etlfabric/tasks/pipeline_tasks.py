from etlfabric.tasks.celery_app import celery_app


@celery_app.task(bind=True, name="etlfabric.run_pipeline")
def run_pipeline(self, pipeline_id: str, org_id: str):
    """Execute a dlt pipeline. Implementation in Phase 2."""
    raise NotImplementedError("Pipeline execution will be implemented in Step 7")


@celery_app.task(bind=True, name="etlfabric.run_transformation")
def run_transformation(self, transformation_id: str, org_id: str):
    """Execute a dbt transformation. Implementation in Phase 3."""
    raise NotImplementedError("Transformation execution will be implemented in Step 10")
