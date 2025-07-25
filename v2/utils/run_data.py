import time
from datetime import datetime
from typing import List
from kfp_server_api import V2beta1Run

class StateMixin:
    @property
    def pending(self) -> bool:
        return self.state == "PENDING"

    @property
    def running(self) -> bool:
        return self.state == "RUNNING"

    @property
    def succeeded(self) -> bool:
        return self.state == "SUCCEEDED"

    @property
    def failed(self) -> bool:
        return self.state == "FAILED"

    @property
    def skipped(self) -> bool:
        return self.state == "SKIPPED"

    @property
    def finished(self) -> bool:
        return self.state in ("SUCCEEDED", "FAILED", "SKIPPED")

class NodeData(StateMixin):
    def __init__(
        self,
        task: dict
    ):
        self.task = task

    @property
    def display_name(self) -> str:
        return self.task.display_name

    @property
    def create_time(self) -> datetime:
        return self.task.create_time

    @property
    def end_time(self) -> datetime:
        return self.task.end_time

    @property
    def start_time(self) -> datetime:
        return self.task.start_time

    @property
    def state(self) -> str:
        return self.task.state

    def __str__(self):
        return f"Node(name={self.display_name}, state={self.state}, create_time={self.create_time}, end_time={self.end_time})"


class RunData(StateMixin):
    def __init__(self, run: V2beta1Run):
        self.run = run
        self._parse_nodes()

    def _parse_nodes(self):
        self.nodes = []

        # Fun nulls when the fun first starts, yay!
        if self.run.run_details is None:
            return

        for task in self.run.run_details.task_details:
            self.nodes.append(NodeData(
                task=task
            ))

    def get_nodes(self, display_name: str) -> List[NodeData]:
        return list(filter(lambda n: n.display_name == display_name, self.nodes))

    @property
    def state(self) -> str:
        return self.run.state

    @property
    def created_at(self) -> datetime:
        return self.run.created_at

    @property
    def scheduled_at(self) -> datetime:
        return self.run.scheduled_at

    @property
    def finished_at(self) -> datetime:
        return self.run.finished_at

    def all_finished(self) -> bool:
        """
        Kind of silly to have a method for this, but looks like the run object's `state` can finished before all of the task's `state` have finished. This is used to wait for all the data to be updated.

        Returns:
            bool: If the run and all tasks in `run_details` are in a finished state.
        """
        if not self.finished:
            return False

        nodes_finished = all(map(lambda n: n.finished, self.nodes))
        return nodes_finished

    def __str__(self) -> str:
        return f"DAG(state={self.state}, created_at={self.created_at}, finished_at={self.finished_at})"

    def display(self):
        print(self)
        for node in self.nodes:
            print(f"  {node}")

if __name__ == '__main__':
    from kfp.client import Client
    from v2.samples.pipelines.single_no_op import single_no_op

    client = Client()

    run = client.create_run_from_pipeline_func(single_no_op)

    while True:
        run = client.get_run(run.run_id)
        run_data = RunData(run)

        run_data.display()

        if run_data.all_finished():
            break

        time.sleep(0.1)

    print("---------------")
    print(run_data.get_nodes('no-op')[0])

    if False:
        print(dir(run))
        for attr in ('created_at', 'finished_at', 'scheduled_at', 'pipeline_spec', 'run_details', 'runtime_config', 'state', 'state_history'):
            print(f"{attr}: {getattr(run, attr)}")
