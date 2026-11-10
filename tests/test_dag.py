import os

import pytest

from streamhouse.config import ROOT

os.environ.setdefault("AIRFLOW_HOME", str(ROOT / "lake" / "airflow"))  # keep Airflow's files inside the repo
pytest.importorskip("airflow")


def test_dag_imports_and_runs_expiry_only_after_the_dq_gate():
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=str(ROOT / "dags"))
    assert bag.import_errors == {}
    dag = bag.dags["streamhouse_maintenance"]
    assert {t.task_id for t in dag.tasks} == {"compact", "rebuild_gold", "dq_checks", "expire"}
    assert dag.get_task("compact").downstream_task_ids == {"rebuild_gold"}
    assert dag.get_task("rebuild_gold").downstream_task_ids == {"dq_checks"}
    assert dag.get_task("dq_checks").downstream_task_ids == {"expire"}
    assert dag.get_task("dq_checks").retries == 0
