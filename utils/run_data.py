from __future__ import annotations

import json
import tarfile
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List
from io import BytesIO
from base64 import b64decode


from kfp import Client
# TODO: This should honestly be annotated in the KFP client
from kfp_server_api.models import ApiRunDetail
from kfp_server_api.models import ApiPipelineRuntime

def utc_now() -> timezone:
    return datetime.now(tz=timezone.utc)

def parse_datetime(dt_str: str) -> datetime:
    dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
    assert dt_str.endswith("Z"), "Does not appear to be Zulu (UTC) timestamp"

    return dt.replace(tzinfo=timezone.utc)

class StatusMixin:
    @property
    def pending(self) -> bool:
        return self.status == 'Pending'

    @property
    def running(self) -> bool:
        return self.status == 'Running'

    @property
    def succeeded(self) -> bool:
        return self.status == 'Succeeded'

    @property
    def failed(self) -> bool:
        return self.status == 'Failed'

class NodeData(StatusMixin):
    def __init__(
        self,
        display_name: str,
        node: dict,
        run: RunData
    ):
        self.display_name = display_name
        self.node = node
        self.run = run

        self.client = self.run.client

    @property
    def node_id(self) -> str:
        return self.node['id']

    @property
    def started_at(self) -> Optional[datetime]:
        started_at = self.node['startedAt']

        if started_at is None:
            return None

        return parse_datetime(started_at)

    @property
    def finished_at(self) -> Optional[datetime]:
        finished_at = self.node['finishedAt']

        if finished_at is None:
            return None

        return parse_datetime(finished_at)

    @property
    def status(self) -> str:
        return self.node['phase']

    @property
    def message(self) -> Optional[str]:
        return self.node.get('message')

    @property
    def duration(self) -> timedelta:
        """
        The duration of the pipeline **component**. During the component execution, this will be calculated from the UTC time on the client.

        **NOTE:** There is usually a slight decrease in duration when going from the UTC time on the client to the 'finishedAt' reported likely due to Kubeflow taking a second or two to report finished components.

        Returns:
            timedelta: The duration of a current or completed pipeline component.
        """
        started_at = self.started_at
        if started_at is None:
            return timedelta(seconds=0)

        finished_at = self.finished_at
        if finished_at is not None:
            return finished_at - started_at

        return utc_now() - started_at

    def pull_logs(self) -> str:
        return self._pull_artifact('main-logs', is_tarfile=False)

    def _pull_artifact(self, artifact_name: str, is_tarfile: bool = True):
        valid_names = [a['name'] for a in self.node['outputs']['artifacts']]
        assert artifact_name in valid_names, "Artifact '%s' not found in artifact names for component: %s" % (artifact_name, valid_names)

        assert self.client is not None, "Pulling artifacts requires access to a KFP client, please provide one while constructing associated 'RunData' object."

        # Reference https://github.com/kubeflow/pipelines/issues/4327#issuecomment-687255001
        artifact = self.client.runs.read_artifact(
            self.run.run_id,
            self.node_id,
            artifact_name
        )

        data = b64decode(artifact.data)

        if not is_tarfile:
            return data.decode()

        # NOTE: Honestly not sure which stuff is a tarfile, know I had some instances previously.
        buffer = BytesIO()
        buffer.write(data)
        buffer.seek(0)
        with tarfile.open(fileobj=buffer) as tar:
            member_names = tar.getnames()
            data = {}
            for name in member_names:
                data[name] = tar.extractfile(name).read().decode('utf-8')

        return data

    def __str__(self) -> str:
        return f"Node(name={self.display_name}, status={self.status}, duration={self.duration})"

class RunData:
    @classmethod
    def from_run_detail(
        cls,
        run_detail: ApiRunDetail,
        client: Optional[Client] = None
    ):
        return cls.from_pipeline_runtime(
            pipeline_runtime=run_detail.pipeline_runtime,
            client=client
        )

    @classmethod
    def from_pipeline_runtime(
        cls,
        pipeline_runtime: ApiPipelineRuntime,
        client: Optional[Client] = None
    ):
        workflow_manifest = json.loads(pipeline_runtime.workflow_manifest)
        return cls(
            workflow_manifest=workflow_manifest,
            client=client
        )

    def __init__(
        self,
        workflow_manifest: dict,
        client: Optional[Client] = None
    ):
        self.workflow_manifest = workflow_manifest
        self.client = client

        self._parse_nodes()

    def _parse_nodes(self):
        self.nodes: Dict[str, NodeData] = {}

        # At the very start of runs, no nodes will be present
        if 'nodes' not in self.workflow_manifest['status']:
            return

        # Key templates by name
        templates = {}
        for template in self.workflow_manifest['spec']['templates']:
            templates[template['name']] = template

        # Parse ONLY nodes which represent Pods (not Argo's 'DAG', or 'TaskGroup')
        for name, node in self.workflow_manifest['status']['nodes'].items():
            if node['type'] != 'Pod':
                continue

            display_name = templates[node['templateName']]["metadata"]["annotations"].get(
                "pipelines.kubeflow.org/task_display_name",
                node['displayName']
            )

            # NOTE: Important that we are using the Argo node name, not Kubeflow display name which may not be unique
            self.nodes[name] = NodeData(
                display_name=display_name,
                node=node,
                run=self
            )

    @property
    def run_name(self) -> str:
        return self.workflow_manifest['metadata']['annotations']['pipelines.kubeflow.org/run_name']

    @property
    def run_id(self) -> str:
        return self.workflow_manifest['metadata']['labels']['pipeline/runid']

    @property
    def started_at(self) -> Optional[datetime]:
        started_at = self.workflow_manifest['status']['startedAt']

        if started_at is None:
            return None

        return parse_datetime(started_at)

    @property
    def finished_at(self) -> Optional[datetime]:
        finished_at = self.workflow_manifest['status']['finishedAt']

        if finished_at is None:
            return None

        return parse_datetime(finished_at)

    @property
    def status(self) -> Optional[str]:
        return self.workflow_manifest['status'].get('phase')

    @property
    def duration(self) -> timedelta:
        """
        The duration of the pipeline **run**. During the pipeline run, this will be calculated from the UTC time on the client.

        **NOTE:** There is usually a slight decrease in duration when going from the UTC time on the client to the 'finishedAt' reported likely due to Kubeflow taking a second or two to report finished runs.

        Returns:
            timedelta: The duration of a current or completed pipeline run.
        """
        started_at = self.started_at
        if started_at is None:
            return timedelta(seconds=0)

        finished_at = self.finished_at
        if finished_at is not None:
            return finished_at - started_at

        return utc_now() - started_at

    def get_nodes(self, display_name: str) -> List[NodeData]:
        return list(
            filter(lambda n: n.display_name == display_name, self.nodes.values())
        )

    def __str__(self) -> str:
        return f"DAG(name={self.run_name}, status={self.status}, duration={self.duration})"

    def display(self):
        print(self)
        for node in self.nodes.values():
            print(f"  {node}")

if __name__ == '__main__':
    import time

    from kfp import Client

    from dump import dump_manifests
    from samples.pipelines import simple_timed, errors

    client = Client()
    print("Creating run...")
    if False: 
        result = client.create_run_from_pipeline_func(
            simple_timed,
            arguments={
                "base_time": 3
            }
        )
    if True:
        result = client.create_run_from_pipeline_func(
            errors,
            arguments={}
        )

    while True:
        # NOTE: The KFP client is returning different types than the hint
        run_detail: ApiRunDetail = client.get_run(result.run_id)
        # dump_manifests('run_data', run_detail)

        data = RunData.from_run_detail(run_detail, client=client)

        data.display()
        print('\n')

        if data.status in ('Succeeded', 'Failed'):
            break

        time.sleep(0.1)

    node = data.get_nodes('runtime-exception')[0]
    print(node.pull_logs())