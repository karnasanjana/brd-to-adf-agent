"""Dataflow-script linter: catch in milliseconds what ADF's Spark broker would
reject 5 minutes into a run (DF-DSL-001 parse errors, DF-AGG-005 duplicates,
dangling stream references).

Runs automatically inside preflight — it is not a user-facing gate but the
application checking its own output the way ADF will, so a bad script never
leaves the machine. Every rule here corresponds to a failure actually observed
in `artifacts/*/heals.json`.
"""

from __future__ import annotations

import re
from typing import List

# X, Y transform(...) ~> Name   — captures input streams (lhs) and output (rhs).
_STEP_RE = re.compile(r"^([\w, ]+?)\s+(\w+)\(", re.MULTILINE)
# Words that actually start a transform body. Anything else matching _STEP_RE is
# script content, not a step — e.g. a typed source column `line_total as
# decimal(18, 2)` would otherwise parse as stream 'line_total as' + step
# 'decimal(' (false positive observed on the bike-store BRD).
_KNOWN_STEPS = {
    "source", "sink", "derive", "filter", "select", "window", "sort",
    "aggregate", "join", "rank", "union", "exists", "alterRow", "keyGenerate",
    "pivot", "unpivot", "lookup", "flatten", "surrogateKey", "cast",
}
_OUT_RE = re.compile(r"~>\s*(\w+)\s*$", re.MULTILINE)
_EMPTY_BODY_RE = re.compile(r"\b(derive|filter|select|window|sort|rank)\(\s*\)")
# ADF has no limit() transform — DF-FLO-005 "Transformation 'limit' not
# registered" (run: flow_gold_top_customers). Top-N = rank(...) + filter.
_LIMIT_RE = re.compile(r"\blimit\(")
_EMPTY_GROUPBY_RE = re.compile(r"\bgroupBy\(\s*[),]")
# aggregate(groupBy(x),   <nothing but whitespace>  ) — empty aggregate section.
_EMPTY_AGGS_RE = re.compile(r"\baggregate\((?:groupBy\([^)]*\))?\s*,?\s*\)")
# Spark SQL window syntax that LLMs leak into ADF DSL (DF-DSL-001, run 24756f0f5aa8).
# ADF: window(over(<cols>), asc(<col>, true)/desc(<col>, true), <out> = <fn>()).
_SPARK_TOKEN_RE = re.compile(r"\b(partitionBy|orderBy|row_number)\s*\(")


def _strip_strings(text: str) -> str:
    """Remove single-quoted literals so quoted parens/commas don't confuse checks."""
    return re.sub(r"'(?:[^'\\]|\\.)*'", "''", text)


def lint_dataflow_script(name: str, script: str) -> List[str]:
    """Problems that WILL fail in ADF; empty list = clean."""
    problems: List[str] = []
    if not script.strip():
        return [f"{name}: script is empty"]
    bare = _strip_strings(script)

    # 1. Parenthesis balance (the classic DF-DSL-001).
    depth = 0
    for i, ch in enumerate(bare):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if depth < 0:
            line = bare.count("\n", 0, i) + 1
            problems.append(f"{name}: unbalanced ')' at line {line}")
            break
    if depth > 0:
        problems.append(f"{name}: {depth} unclosed '('")

    # 2. Empty transform bodies — ADF's parser rejects them all.
    for m in _EMPTY_BODY_RE.finditer(bare):
        problems.append(f"{name}: empty {m.group(1)}() body")
    if _EMPTY_GROUPBY_RE.search(bare):
        problems.append(f"{name}: empty groupBy()")
    if _EMPTY_AGGS_RE.search(bare):
        problems.append(f"{name}: aggregate() with no aggregate columns")
    if _LIMIT_RE.search(bare):
        problems.append(
            f"{name}: 'limit(' is not an ADF transform (DF-FLO-005 'limit' not "
            "registered) - keep top N with rank(desc(<sort col>), "
            "output(xRowRank as long), dense: false) then filter(xRowRank <= N)"
        )

    # 3. Stream graph: every input stream must be defined before use; >=1 sink.
    defined = set()
    for line in script.split("\n"):
        out = _OUT_RE.search(line)
        step = _STEP_RE.match(line.strip())
        if step and step.group(2) not in _KNOWN_STEPS:
            step = None  # a typed column or expression line, not a transform step
        if step and step.group(2) != "source":
            for inp in step.group(1).split(","):
                inp = inp.strip()
                if inp and inp not in defined:
                    problems.append(f"{name}: stream '{inp}' used before it is defined")
        if out:
            defined.add(out.group(1))
    if "sink(" not in script:
        problems.append(f"{name}: no sink() step")

    # 4. Window transform written in Spark syntax instead of ADF DSL
    #    (DF-DSL-001 "no viable alternative", run 24756f0f5aa8).
    for m in _SPARK_TOKEN_RE.finditer(bare):
        problems.append(
            f"{name}: '{m.group(1)}(' is Spark syntax, not ADF DSL - window uses "
            "over(...) to partition and asc(col, true)/desc(col, true) to sort"
        )
    for seg in bare.split("~>"):
        if re.search(r"\bwindow\(", seg) and not re.search(r"\bover\(", seg):
            problems.append(f"{name}: window() has no over(...) partition clause")

    # 5. row_num consumed but no window ever creates it
    #    (DF-EXPR-010 "column unavailable", run 24756f0f5aa8).
    if re.search(r"\brow_num\b", bare) and "rowNumber()" not in script:
        problems.append(f"{name}: 'row_num' referenced but no rowNumber() window creates it")

    # 6. Duplicate output columns in aggregate: groupBy cols re-declared as aggs
    #    (DF-AGG-005). Builder guards this; lint re-checks the rendered truth.
    for m in re.finditer(r"aggregate\(groupBy\(([^)]*)\),\s*(.*?)\)\s*~>", bare, re.DOTALL):
        groups = {g.strip() for g in m.group(1).split(",") if g.strip()}
        assigns = {a.split("=")[0].strip() for a in m.group(2).split(",") if "=" in a}
        dup = groups & assigns
        if dup:
            problems.append(f"{name}: aggregate re-declares groupBy column(s): {', '.join(sorted(dup))}")

    return problems
