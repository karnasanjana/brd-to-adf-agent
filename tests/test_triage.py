"""Failure triage + plan-phase lesson loop (agent/triage.py).

Questions: residual problems become answerable chat questions.
Lessons: repaired problems group by model-independent signature; a signature
seen twice distills into one generic rule (LLM stubbed here).
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent import triage


def test_signature_is_model_and_name_independent():
    a = triage.signature(
        "output 'monthly_sales': 'order_year' is not a column of ['orders'] (has: ['a', 'b'])"
    )
    b = triage.signature(
        "output 'sales_trend': 'net_sales' is not a column of ['orders', 'items'] (has: ['x'])"
    )
    assert a == b
    assert "monthly_sales" not in a and "order_year" not in a


def test_signature_collapses_numbers():
    assert triage.signature("row 12 failed") == triage.signature("row 99 failed")


def test_questions_are_answerable_and_capped():
    qs = triage.questions_for(
        [f"output 'o{i}': 'c{i}' is not a column of ['e']" for i in range(10)]
    )
    assert len(qs) == 5
    col_q = triage.question_for("output 'x': 'y' is not a column of ['z']")
    assert "which existing column" in col_q


def test_internal_defects_are_not_routed_as_user_questions():
    """Over-routing guard: a problem the user can't answer gets ONE honest
    retry/report line, not a fake question."""
    qs = triage.questions_for(["compiler emitted duplicate stream name src1"])
    assert len(qs) == 1
    assert "look internal" in qs[0]
    assert "retry" in qs[0]


def test_mixed_problems_split_user_and_internal():
    qs = triage.questions_for([
        "output 'a': 'b' is not a column of ['c']",   # user-answerable
        "internal stream ordering defect",              # internal
    ])
    assert len(qs) == 2
    assert "which existing column" in qs[0]
    assert "look internal" in qs[1]


def test_record_and_ready_groups(tmp_path):
    root = str(tmp_path)
    p1 = "output 'a': 'c1' is not a column of ['e'] (has: ['x'])"
    p2 = "output 'b': 'c2' is not a column of ['f'] (has: ['y'])"
    assert triage.record_candidates([p1], root=root) == 1
    assert triage.ready_groups(root=root) == []  # seen once — not ready
    triage.record_candidates([p2], root=root)
    groups = triage.ready_groups(root=root)
    assert len(groups) == 1 and groups[0]["count"] == 2


def test_distill_marks_signature_done_and_appends_rule(tmp_path, monkeypatch):
    root = str(tmp_path)
    skills_dir = str(tmp_path / "skills")
    triage.record_candidates(["cast 'a' invalid"], root=root)
    triage.record_candidates(["cast 'b' invalid"], root=root)

    from agent import skills

    def fake_lesson(example, skills_dir=skills.lessons_path.__defaults__[0]):
        os.makedirs(os.path.dirname(skills.lessons_path(skills_dir)), exist_ok=True)
        with open(skills.lessons_path(skills_dir), "a", encoding="utf-8") as fh:
            fh.write("- Always cast used columns.\n")
        return "Always cast used columns."

    monkeypatch.setattr(skills, "record_plan_lesson", fake_lesson)
    rules = triage.distill_ready(root=root, skills_dir=skills_dir)
    assert rules == ["Always cast used columns."]
    assert triage.ready_groups(root=root) == []  # marked distilled
    # A second pass distills nothing (idempotent).
    assert triage.distill_ready(root=root, skills_dir=skills_dir) == []


def test_candidate_metadata_keeps_model_out_of_signature(tmp_path, monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "some-model-id")
    root = str(tmp_path)
    triage.record_candidates(["op 'foo' unknown"], root=root)
    with open(triage.candidates_path(root), "r", encoding="utf-8") as fh:
        rec = json.loads(fh.readline())
    assert rec["model"] == "some-model-id"  # audit field only
    assert "some-model-id" not in rec["signature"]
