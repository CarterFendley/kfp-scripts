from __future__ import annotations
import json
import tarfile
from io import BytesIO
from base64 import b64decode
from datetime import datetime
from dataclasses import dataclass
from typing import List
from functools import cache

from kfp import Client
from graphviz import Digraph

KFP_TYPE_MAP = {
    "Integer": int,
    "Float": float,
    "Boolean": bool,
    "String": str,
    "JsonObject": json.loads
}

def get_node_color(node_type: str):
    if node_type == 'Pod':
        return 'aqua'
    else:
        return 'azure3'


def get_artifact(
    client: Client,
    run_id: str,
    node_id: str,
    artifact_name: str,
) -> dict:
    """
    Source: https://github.com/kubeflow/pipelines/issues/4327#issuecomment-687255001
    """
    artifact = client.runs.read_artifact(
        run_id,
        node_id,
        artifact_name
    )
    data = b64decode(artifact.data)

    buffer = BytesIO()
    buffer.write(data)
    buffer.seek(0)
    with tarfile.open(fileobj=buffer) as tar:
        member_names = tar.getnames()
        data = {}
        for name in member_names:
            data[name] = tar.extractfile(name).read().decode('utf-8')

    return data

@dataclass
class KFPRun:
    runtime_manifest: dict
    _client: Client = None

    def __post_init__(self):
        self._metadata = self.runtime_manifest['metadata']

    @property
    def run_id(self) -> str:
        return self._metadata['labels']['pipeline/runid']

    @property
    def run_name(self) -> str:
        return self._metadata['annotations']['pipelines.kubeflow.org/run_name']

    @property
    def phase(self) -> str:
        return self._metadata['labels']['workflows.argoproj.io/phase']

    @property
    def started_at(self) -> datetime:
        #return datetime.fromisoformat(self.runtime_manifest['status']['startedAt'])
        return self.runtime_manifest['status']['startedAt']
    @property
    def finished_at(self) -> datetime:
        #return datetime.fromisoformat(self.runtime_manifest['status']['finishedAt'])
        return self.runtime_manifest['status']['finishedAt']

    def get_pod_nodes(self) -> List[KFPPodNode]:
        pod_nodes = []
        for node in self.runtime_manifest['status']['nodes'].values():
            if node['type'] == 'Pod':
                pod_nodes.append(KFPPodNode(run=self, node=node))

        return pod_nodes

    def get_node(self, id: str) -> KFPPodNode:
        return KFPPodNode(
            run=self,
            node=self.runtime_manifest['status']['nodes'][id]
        )

    def graph_viz(self, raw=True) -> Digraph:
        dot = Digraph(self.runtime_manifest['metadata']['name'])

        edges = []
        for name, node in self.runtime_manifest['status']['nodes'].items():

            dot.node(
                name=name, 
                label=f"{node['displayName']} ({name})",
                tooltip=json.dumps(node, indent=4),
                style='filled',
                fillcolor=get_node_color(node['type'])
            )

            if 'children' in node:
                for child in node['children']:
                    edges.append((name, child))

        for edge in edges:
            dot.edge(*edge)

        return dot

    def to_record(self) -> dict:
        return {
            'run_id': self.run_id,
            'run_name': self.run_name,
            'run_status': self.phase,
            'run_start': self.started_at,
            'run_finish': self.finished_at
        }

@dataclass
class KFPPodNode:
    run: KFPRun
    node: dict

    @property
    def node_id(self) -> str:
        return self.node['id']

    @property
    def template_name(self) -> str:
        return self.node['templateName']

    @property
    def node_template(self) -> dict:
        for template in self.run.manifest['spec']['templates']:
            if template['name'] == self.template_name:
                return template

        raise RuntimeError(f"Could not find template for '{self.template_name}' in spec.")

    @property
    def component_spec(self) -> dict:
        return json.loads(
            self.node_template['metadata']['annotations']['pipelines.kubeflow.org/component_spec']
        )

    @property
    def outputs(self) -> dict:
        # NOTE: Assuming orders match here
        outputs = self.node_template['outputs']['artifacts']
        spec_outputs = self.component_spec["outputs"]

        combined = {}
        for o, so in zip(outputs, spec_outputs):
            full_name = o['name']
            combined[full_name] = {
                'path': o['path'],
                'short_name': so['name'],
                'type': so['type']
            }

        return combined

    def to_record(self) -> dict:
        record = {
            'stage_name': self.node['displayName']
        }
        record.update(self.run.to_record())

        return record

    def get_output_data(self, normalize=True):
        assert self.run._client is not None, "Could not find KFP client."
        client = self.run._client

        outputs = self.outputs
        data = {}
        for artifact in self.node['outputs']['artifacts']:
            # Skip anything that is not a output
            if artifact['name'] not in outputs:
                continue

            datum = get_artifact(
                client=client,
                run_id=self.run.run_id,
                node_id=self.node_id,
                artifact_name=artifact['name']
            )
            datum = datum['data']

            t = KFP_TYPE_MAP[outputs[artifact['name']]['type']]
            data[artifact['name']] = t(datum)

        # Normals names by removing template name
        if normalize:
            data = {
                k.replace(f'{self.template_name}-', ''):v
                for k, v in data.items()
            }

        return data