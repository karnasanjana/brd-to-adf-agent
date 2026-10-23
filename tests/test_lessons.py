"""Learned-lessons loop: self-heal incidents distill into skills/99-learned-lessons.md,
which the skill loader feeds back to the planner — knowledge grows without code."""

import config
from agent import skills


class _FakeOutput:
    def __init__(self, covered, lesson):
        self.covered = covered
        self.lesson = lesson


class _FakeResult:
    def __init__(self, output):
        self.output = output


def _fake_agent_returning(covered, lesson):
    class _FakeAgent:
        def __init__(self, *args, **kwargs):
            pass

        def run_sync(self, *args, **kwargs):
            return _FakeResult(_FakeOutput(covered, lesson))

    return _FakeAgent


def _patch_llm(monkeypatch, covered, lesson):
    import pydantic_ai

    monkeypatch.setattr(pydantic_ai, "Agent", _fake_agent_returning(covered, lesson))
    monkeypatch.setattr(config, "get_model", lambda: None)
    monkeypatch.setattr(config, "structured_output", lambda cls: cls)


def test_record_lesson_appends_and_loader_picks_up(monkeypatch, tmp_path):
    skills_dir = str(tmp_path / "skills")
    rule = "Coalesce defaults must match the column type (DF-DT-001)."
    _patch_llm(monkeypatch, covered=False, lesson=rule)

    assert skills.record_lesson("DF-DT-001 ...", "cast default to string", skills_dir) == rule
    loaded = skills.load_skills(skills_dir)
    assert rule in loaded

    # Same failure class again: model says covered -> no duplicate line.
    _patch_llm(monkeypatch, covered=True, lesson="")
    assert skills.record_lesson("DF-DT-001 again", "same fix", skills_dir) is None
    with open(skills.lessons_path(skills_dir), encoding="utf-8") as fh:
        assert fh.read().count(rule) == 1


def test_record_lesson_never_raises(monkeypatch, tmp_path):
    """LLM blowing up must not break the heal that triggered the lesson."""

    class _Boom:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("no model configured")

    import pydantic_ai

    monkeypatch.setattr(pydantic_ai, "Agent", _Boom)
    assert skills.record_lesson("err", "fix", str(tmp_path / "skills")) is None
