import json

from kfp_server_api.models import ApiRun
from kfp_server_api.models import ApiRunDetail
from kfp_server_api.models import ApiPipelineRuntime
from kfp_server_api.models import ApiPipelineSpec

from utils.manifest import KFPRun

def print_run_info(run: ApiRun):
    print("Run details:", run.name, run.id )
    print("===========================")
    print("Final status:", run.status)
    print("Pipeline timing:")
    print("- Created at:", run.created_at)
    print("- Scheduled at:", run.scheduled_at) # TODO: Why is this 1970
    print("- Finished at:", run.finished_at)

def dump_manifests(name: str, run_detail: ApiRunDetail):
    pipeline_runtime: ApiPipelineRuntime = run_detail.pipeline_runtime
    print(">>> Writing pipeline runtime to pipeline_runtime.workflow_manifest.json")
    with open(f"{name}.pipeline_runtime.workflow_manifest.json", "w") as f:
        import json
        json.dump(
            json.loads(pipeline_runtime.workflow_manifest),
            f,
            indent=2
        )

    run: ApiRun = run_detail.run
    pipline_spec: ApiPipelineSpec = run.pipeline_spec
    print(">>> Writing pipeline spec to pipeline_spec.workflow_manifest.json")
    with open(f"{name}.pipeline_spec.workflow_manifest.json", "w") as f:
        import json
        json.dump(
            json.loads(pipline_spec.workflow_manifest),
            f,
            indent=2
        )

    assert pipline_spec.pipeline_manifest is None, "Pipeline manifest is not none, dumping is not implemented"
    assert pipline_spec.runtime_config is None, "Runtime config is not none, dumping is not implemented"


def dump_graphviz(pipeline_runtime: ApiPipelineRuntime, view: bool):
    kfp_run = KFPRun(
        runtime_manifest=json.loads(pipeline_runtime.workflow_manifest),
    )

    dot = kfp_run.graph_viz()
    print(">>> Writing pipeline runtime to pipeline_runtime.png")
    dot.render('pipeline_runtime.png', view=view)
