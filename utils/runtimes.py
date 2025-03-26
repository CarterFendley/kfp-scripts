import json
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List

from kfp import Client
# TODO: This should honestly be annotated in the KFP client
from kfp_server_api.models import ApiRunDetail
from kfp_server_api.models import ApiPipelineRuntime

from samples.pipelines import simple_timed
from utils.dump import dump_manifests, print_run_info, dump_graphviz
from utils.manifest import KFPRun

def parse_datetime(dt_str: str) -> datetime:
    return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")

@dataclass
class Node:
    name: str
    display_name: str
    stated_at: datetime
    finished_at: datetime
    duration: timedelta = None

    def __post_init__(self):
        self.duration = self.finished_at - self.stated_at

    def display(self):
        print(self.display_name)
        print("=======================")
        print("Started at:", self.stated_at)
        print("Finished at:", self.finished_at)
        print("Duration:", self.duration)

class RuntimeAccessor:
    @classmethod
    def from_run_detail(cls, run_detail: ApiRunDetail):
        return cls.from_pipeline_runtime(run_detail.pipeline_runtime)

    @classmethod
    def from_pipeline_runtime(cls, pipeline_runtime: ApiPipelineRuntime):
        workflow_manifest = json.loads(pipeline_runtime.workflow_manifest)
        return cls(workflow_manifest)

    def __init__(self, workflow_manifest: dict):
        self.workflow_manifest = workflow_manifest

        # Key templates by name
        self.templates = {}
        for template in workflow_manifest['spec']['templates']:
            self.templates[template['name']] = template

        # Load nodes into dataclass upfront
        self.nodes = {}
        for name, node in workflow_manifest['status']['nodes'].items():
            if node['type'] != 'Pod':
                continue

            # TODO: Pain in the butt
            display_name = self.templates[node['templateName']]["metadata"]["annotations"].get(
                "pipelines.kubeflow.org/task_display_name",
                node['displayName']
            )

            self.nodes[name] = Node(
                name=name,
                display_name=display_name,
                stated_at=parse_datetime(node['startedAt']),
                finished_at=parse_datetime(node['finishedAt']),
            )

    def get_by_name(self, display_name: str) -> List[Node]:
        matches = []
        for node in self.nodes.values():
            if display_name == node.display_name:
                matches.append(node)
        return matches

    def display_all(self):
        for node in self.nodes.values():
            node.display()
            print()

def run():
    client = Client()
    print("Creating run...")
    result = client.create_run_from_pipeline_func(
        simple_timed,
        arguments={
            "base_time": 3
        }
    )

    print("Waiting for run to complete...")
    run_detail: ApiRunDetail = result.wait_for_run_completion(300)

    print()
    print_run_info(run_detail.run)
    dump_manifests(run_detail)
    dump_graphviz(run_detail.pipeline_runtime, view = False)

    print()
    runtimes = RuntimeAccessor.from_run_detail(run_detail)
    s1 = runtimes.get_by_name("Sequential 1")[0]
    print("Sequential 1:")
    print("\tduration:", s1.duration)

    s1 = runtimes.get_by_name("Sequential 2")[0]
    print("Sequential 2:")
    print("\tduration:", s1.duration)


if __name__ == "__main__":
    if True:
        run()
    else:
        with open("pipeline_runtime.workflow_manifest.json", "r") as f:
            workflow_manifest = json.load(f)
        runtimes = RuntimeAccessor(workflow_manifest)
        runtimes.display_all()

