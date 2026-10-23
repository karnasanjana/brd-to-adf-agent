"""BRD-named layer containers: 'copy into the raw container' must actually
land in raw/ — not the hardcoded medallion names."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.compiler import compile_plan
from agent.planhandlers import PlanSession
from models.logical import (
    AggregationPlan,
    ContainerPlan,
    EntityTransform,
    LogicalColumn,
    LogicalPlan,
    LogicalSource,
    OutputPlan,
)


def _plan(**kw):
    return LogicalPlan(
        project="pop",
        sources=[LogicalSource(entity="population", format="csv", file_name="population.csv",
                               columns=[LogicalColumn(name="country"),
                                        LogicalColumn(name="value", type="double")])],
        transforms=[EntityTransform(entity="population", dedup_keys=["country"])],
        outputs=[OutputPlan(name="by_country", from_entities=["population"],
                            group_by=["country"],
                            aggregations=[AggregationPlan(name="total", op="sum", column="value")])],
        **kw,
    )


def _locations(spec):
    return {d.name: d.location.container for d in spec.datasets if d.location}


def test_brd_named_containers_used_everywhere():
    spec = compile_plan(_plan(containers=ContainerPlan(
        bronze="raw", silver="curated", gold="analytics")))
    locs = _locations(spec)
    assert locs["ds_bronze_population"] == "raw"
    assert locs["ds_silver_population"] == "curated"
    assert locs["ds_gold_by_country"] == "analytics"
    assert {"raw", "curated", "analytics"} <= set(spec.storage.containers)
    assert any("bronze -> 'raw'" in a for a in spec.assumptions)


def test_default_containers_unchanged():
    spec = compile_plan(_plan())
    locs = _locations(spec)
    assert locs["ds_bronze_population"] == "bronze"
    assert locs["ds_silver_population"] == "silver"
    assert locs["ds_gold_by_country"] == "gold"
    assert not any("Layer containers from the BRD" in a for a in spec.assumptions)


def test_invalid_container_name_rejected():
    with pytest.raises(Exception) as exc:
        ContainerPlan(bronze="Raw_Zone!")
    assert "container name" in str(exc.value)


def test_container_name_normalized_lowercase():
    assert ContainerPlan(bronze="RAW").bronze == "raw"


def test_session_roundtrips_containers():
    plan = _plan(containers=ContainerPlan(bronze="raw"))
    s = PlanSession.from_plan(plan)
    assert s.plan().containers.bronze == "raw"
    s2 = PlanSession()
    s2.set_containers(bronze="raw", gold="analytics")
    c = s2.plan().containers
    assert (c.bronze, c.silver, c.gold) == ("raw", "silver", "analytics")


def test_set_containers_rejects_bad_name_via_toolresult():
    s = PlanSession()
    res = s.set_containers(bronze="BAD NAME")
    assert res["ok"] is False
