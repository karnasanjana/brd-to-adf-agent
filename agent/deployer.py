"""Deployer: provision ADLS containers, then create_or_update every ADF object.

Runs ONLY after the human approval gate (and only after pre-flight passes).
Everything is idempotent — re-running converges to the same state.

Two backends:
  * REAL (default): azure-storage-file-datalake + azure-mgmt-datafactory, using
    DefaultAzureCredential (Managed Identity / service principal). No keys.
  * MOCK (MOCK=true): writes the ARM JSON into mock_tenant/ and records the
    "containers". Used for the local dry-run / Azure-free development. Clearly
    labelled in the UI; OFF by default.
"""

from __future__ import annotations

import json
import os
import shutil
from typing import Any, Dict, List

from agent.builder import DEPLOY_ORDER
from config import MOCK_TENANT_DIR, AzureConfig, mock_mode

_RESOURCE_CALL = {
    "integrationRuntimes": "integration_runtimes",
    "linkedServices": "linked_services",
    "datasets": "datasets",
    "dataflows": "data_flows",
    "pipelines": "pipelines",
    "triggers": "triggers",
}


def _prune_enabled() -> bool:
    """Whether to delete AGENT-CREATED factory objects not in the current run.

    OFF by default: the agent never deletes Azure objects on its own — only the
    user does, deliberately, by setting PRUNE_STALE=true. When off, the deploy
    still REPORTS the stale agent-created objects a prune would remove (see
    last_stale / the UI pill) so the user can clean up themselves. Scope guard
    when enabled: only objects matching this agent's naming convention (see
    _prunable) are ever deleted — a shared factory's unrelated objects are
    never touched."""
    return os.environ.get("PRUNE_STALE", "false").strip().lower() in {"true", "1", "yes", "on"}


# Every object the agent creates carries one of these category prefixes (compiler
# naming formulas + spec.py _PREFIXES). Prune ONLY considers matching names, so
# manually created / unrelated objects in a shared factory are never deleted.
_AGENT_PREFIXES = {
    "integrationRuntimes": "ir-",  # IR names reject underscores (InvalidName)
    "linkedServices": "ls_",
    "datasets": "ds_",
    "dataflows": "df_",
    "pipelines": "pl_",
    "triggers": "tr_",
}


def _prunable(category: str, name: str) -> bool:
    return name.startswith(_AGENT_PREFIXES.get(category, "\x00"))


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #


def _load_category(run_dir: str, category: str) -> Dict[str, Dict[str, Any]]:
    cat_dir = os.path.join(run_dir, category)
    objects: Dict[str, Dict[str, Any]] = {}
    if not os.path.isdir(cat_dir):
        return objects
    for fname in sorted(os.listdir(cat_dir)):
        if fname.endswith(".json"):
            with open(os.path.join(cat_dir, fname), "r", encoding="utf-8") as fh:
                doc = json.load(fh)
            objects[doc["name"]] = doc["properties"]
    return objects


def _pipeline_refs(props: Dict[str, Any]) -> set:
    """Pipelines this pipeline invokes via ExecutePipeline (its dependencies)."""
    refs = set()
    for act in props.get("activities", []):
        if act.get("type") == "ExecutePipeline":
            ref = act.get("typeProperties", {}).get("pipeline", {}).get("referenceName")
            if ref:
                refs.add(ref)
    return refs


def _ordered_items(category: str, objects: Dict[str, Dict[str, Any]]):
    """Deployment order within a category. Pipelines need a topological sort so a
    pipeline is created AFTER the pipelines it references via ExecutePipeline —
    otherwise ADF rejects the master with 'invalid reference'. Other categories
    keep their natural (sorted) order."""
    if category != "pipelines":
        return list(objects.items())

    remaining = dict(objects)
    placed: set = set()
    ordered = []
    while remaining:
        progressed = False
        for name in list(remaining):
            deps = _pipeline_refs(remaining[name]) & set(objects)
            if deps <= placed:  # all internal deps already queued
                ordered.append((name, remaining.pop(name)))
                placed.add(name)
                progressed = True
        if not progressed:  # cycle / external ref — append the rest as-is
            ordered.extend(remaining.items())
            break
    return ordered


# --------------------------------------------------------------------------- #
# MOCK backend
# --------------------------------------------------------------------------- #


def _mock_factory_dir(cfg: "AzureConfig | None") -> str:
    factory = cfg.factory_name if cfg else os.environ.get("ADF_FACTORY_NAME", "mock-factory")
    return os.path.join(MOCK_TENANT_DIR, factory)


def _mock_provision(containers: List[str]) -> List[str]:
    base = os.path.join(MOCK_TENANT_DIR, "storage")
    created: List[str] = []
    for name in containers:
        path = os.path.join(base, name)
        if not os.path.isdir(path):
            os.makedirs(path, exist_ok=True)
            created.append(name)
    return created


def _mock_deploy(run_dir: str) -> List[str]:
    factory_dir = _mock_factory_dir(None)
    deployed: List[str] = []
    prune = _prune_enabled()
    for category in DEPLOY_ORDER:
        objects = _load_category(run_dir, category)
        if not objects:
            continue
        out_dir = os.path.join(factory_dir, category)
        os.makedirs(out_dir, exist_ok=True)
        keep = set()
        for name, props in _ordered_items(category, objects):
            with open(os.path.join(out_dir, f"{name}.json"), "w", encoding="utf-8") as fh:
                json.dump({"name": name, "properties": props}, fh, indent=2)
            deployed.append(f"{category}/{name}")
            keep.add(name)
        # Converge: drop stale JSON left by earlier runs with different names
        # (same scope guard as the real backend: agent-prefixed objects only).
        if prune:
            for fname in os.listdir(out_dir):
                if (
                    fname.endswith(".json")
                    and fname[:-5] not in keep
                    and _prunable(category, fname[:-5])
                ):
                    os.remove(os.path.join(out_dir, fname))
    # Carry the data-flow scripts across too, for inspection.
    src_scripts = os.path.join(run_dir, "dataflows")
    if os.path.isdir(src_scripts):
        dst = os.path.join(factory_dir, "dataflows")
        os.makedirs(dst, exist_ok=True)
        for fname in os.listdir(src_scripts):
            if fname.endswith(".script"):
                shutil.copy2(os.path.join(src_scripts, fname), os.path.join(dst, fname))
    # Same verification the real backend reports: intended vs actual counts.
    deploy_artifacts.last_intended_counts = {  # type: ignore[attr-defined]
        cat: len(_load_category(run_dir, cat)) for cat in DEPLOY_ORDER
    }
    deploy_artifacts.last_factory_counts = {  # type: ignore[attr-defined]
        cat: sum(
            1 for f in os.listdir(os.path.join(factory_dir, cat)) if f.endswith(".json")
        )
        if os.path.isdir(os.path.join(factory_dir, cat))
        else 0
        for cat in DEPLOY_ORDER
    }
    return deployed


# --------------------------------------------------------------------------- #
# REAL backend
# --------------------------------------------------------------------------- #


def _real_provision(containers: List[str], cfg: AzureConfig) -> List[str]:
    from azure.core.exceptions import HttpResponseError, ResourceExistsError

    from config import get_blob_service_client, get_datalake_client, storage_kind

    def _create(make) -> "str | None":
        """Create a container/filesystem; treat 'already exists' AND a data-plane
        403 as non-fatal. The deploy identity may lack 'Storage Blob Data
        Contributor' (control-plane roles don't grant data access) while the
        containers already exist and the ADF Managed Identity — which actually reads/
        writes at run time — is separately authorized. Blocking the whole deploy on a
        redundant create step would be wrong, so we skip and let the run proceed."""
        try:
            make()
            return "created"
        except ResourceExistsError:
            return None  # idempotent — already there
        except HttpResponseError as exc:
            blob = f"{exc}".lower()
            if exc.status_code in (401, 403) or "authorizationpermissionmismatch" in blob:
                return None  # no data-plane role; assume pre-existing, don't block deploy
            raise

    created: List[str] = []
    if storage_kind() == "blob":
        # Plain Blob storage (no HNS): containers, not filesystems.
        service = get_blob_service_client(cfg)
        for name in containers:
            if _create(lambda n=name: service.create_container(n)):
                created.append(name)
        return created

    service = get_datalake_client(cfg)
    for name in containers:
        if _create(lambda n=name: service.create_file_system(file_system=n)):
            created.append(name)
    return created


def _real_resource(category: str, properties: Dict[str, Any]):
    from azure.mgmt.datafactory.models import (
        DataFlowResource,
        DatasetResource,
        IntegrationRuntimeResource,
        LinkedServiceResource,
        PipelineResource,
        TriggerResource,
    )

    classes = {
        "integrationRuntimes": IntegrationRuntimeResource,
        "linkedServices": LinkedServiceResource,
        "datasets": DatasetResource,
        "dataflows": DataFlowResource,
        "pipelines": PipelineResource,
        "triggers": TriggerResource,
    }
    return classes[category].from_dict({"properties": properties})


def _real_deploy_object(adf, cfg: AzureConfig, category: str, name: str, properties: Dict[str, Any]):
    resource = _real_resource(category, properties)
    op = getattr(adf, _RESOURCE_CALL[category])
    return op.create_or_update(cfg.resource_group, cfg.factory_name, name, resource)


# Delete in the REVERSE of DEPLOY_ORDER: a dataset can't be removed while a pipeline
# still references it, a pipeline can't go while a trigger points at it, etc.
_PRUNE_ORDER = list(reversed(DEPLOY_ORDER))


def _stale_names(adf, cfg: AzureConfig, keep: Dict[str, set]) -> List[str]:
    """Report-only twin of _real_prune: the agent-created objects a prune WOULD
    delete, without touching anything."""
    stale: List[str] = []
    for category in _PRUNE_ORDER:
        op = getattr(adf, _RESOURCE_CALL[category])
        keep_names = keep.get(category, set())
        stale.extend(
            f"{category}/{item.name}"
            for item in op.list_by_factory(cfg.resource_group, cfg.factory_name)
            if item.name not in keep_names and _prunable(category, item.name)
        )
    return stale


def _real_prune(adf, cfg: AzureConfig, keep: Dict[str, set]) -> List[str]:
    """Delete every factory object whose name is not in ``keep`` (this run's objects),
    so the factory converges to exactly this run. Triggers are stopped before delete —
    ADF refuses to delete a started trigger.

    Within a category, deletion is PASS-BASED: an object still referenced by another
    (a stale master pipeline invoking a stale child via ExecutePipeline) fails its
    delete with 'cannot be deleted since it is referenced by ...'. We retry in passes,
    deleting whatever succeeds each pass; once the referrer is gone the referenced
    object deletes on the next pass. This converges regardless of the stale objects'
    dependency order without fetching each object's graph."""
    removed: List[str] = []
    for category in _PRUNE_ORDER:
        op = getattr(adf, _RESOURCE_CALL[category])
        keep_names = keep.get(category, set())
        pending = [
            item.name
            for item in op.list_by_factory(cfg.resource_group, cfg.factory_name)
            if item.name not in keep_names and _prunable(category, item.name)
        ]
        while pending:
            progressed = False
            blocked: List[str] = []
            for name in pending:
                if category == "triggers":
                    try:
                        adf.triggers.begin_stop(cfg.resource_group, cfg.factory_name, name).result()
                    except Exception:  # noqa: BLE001 — not started / already stopped
                        pass
                try:
                    op.delete(cfg.resource_group, cfg.factory_name, name)
                    removed.append(f"{category}/{name}")
                    progressed = True
                except Exception:  # noqa: BLE001 — likely still referenced; retry next pass
                    blocked.append(name)
            if not progressed:  # nothing deletable this pass — real error, stop looping
                break
            pending = blocked
    return removed


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #


def provision_containers(containers: List[str], cfg: "AzureConfig | None" = None) -> List[str]:
    """Create bronze/silver/gold filesystems. Idempotent. MOCK-aware."""
    if mock_mode():
        return _mock_provision(containers)
    cfg = cfg or AzureConfig.from_env()
    return _real_provision(containers, cfg)


def _intended_keep(run_dir: str) -> Dict[str, set]:
    """The FULL set of object names this run intends to create, per category — read
    from the run dir BEFORE deploying. Prune keeps exactly these, so a partial deploy
    failure can never prune this run's own objects (only stale ones from prior runs)."""
    keep: Dict[str, set] = {}
    for category in DEPLOY_ORDER:
        keep[category] = set(_load_category(run_dir, category).keys())
    return keep


def _factory_counts_real(adf, cfg: AzureConfig) -> Dict[str, int]:
    """What the factory ACTUALLY contains per category, listed live after deploy +
    prune. This is the ground truth the UI compares against the run's intended
    counts — it makes 'the factory has 60 datasets but the plan had 16' visible
    instead of silent."""
    counts: Dict[str, int] = {}
    for category in DEPLOY_ORDER:
        op = getattr(adf, _RESOURCE_CALL[category])
        try:
            counts[category] = sum(
                1 for _ in op.list_by_factory(cfg.resource_group, cfg.factory_name)
            )
        except Exception:  # noqa: BLE001 — verification must never fail the deploy
            counts[category] = -1  # unknown
    return counts


def deploy_artifacts(run_dir: str, cfg: "AzureConfig | None" = None) -> List[str]:
    """Deploy every artifact in dependency order. Idempotent. MOCK-aware.

    With self-heal enabled (default), a deploy-time rejection (malformed ARM body) is sent back
    to Claude once to correct the artifact, then redeployed — the same heal loop
    used at runtime, applied at create_or_update time. Recorded fixes are exposed
    on ``deploy_artifacts.last_heals`` for the UI.

    Convergence (fixes the "objects pile up / created thrice" bug): the keep-set is
    the run's FULL intended object list, computed upfront, and prune runs in a
    ``finally`` — so even if a deploy raises mid-loop, the factory still converges to
    exactly this run and stale names from earlier runs are removed. Prune never
    deletes this run's own objects because they are all in ``keep``.
    """
    if mock_mode():
        return _mock_deploy(run_dir)
    cfg = cfg or AzureConfig.from_env()
    from config import get_datafactory_client

    adf = get_datafactory_client(cfg)
    deployed: List[str] = []
    heals: List[Dict[str, str]] = []
    keep = _intended_keep(run_dir)  # full intended set, known before any write
    deploy_artifacts.last_heals = heals  # type: ignore[attr-defined]
    deploy_artifacts.last_pruned = []  # type: ignore[attr-defined]
    deploy_artifacts.last_stale = []  # type: ignore[attr-defined]
    deploy_artifacts.last_intended_counts = {  # type: ignore[attr-defined]
        cat: len(names) for cat, names in keep.items()
    }
    deploy_artifacts.last_factory_counts = None  # type: ignore[attr-defined]

    from agent.validator import heal_artifact, self_heal_enabled

    try:
        for category in DEPLOY_ORDER:
            for name, properties in _ordered_items(category, _load_category(run_dir, category)):
                try:
                    _real_deploy_object(adf, cfg, category, name, properties)
                except Exception as exc:  # noqa: BLE001 — bad ARM body
                    if not self_heal_enabled():
                        raise
                    fix = heal_artifact(
                        run_dir, category, name, f"{type(exc).__name__}: {exc}", phase="deploy"
                    )
                    heals.append({"category": category, "name": name, "fix": fix})
                    fixed = _load_category(run_dir, category)[name]
                    _real_deploy_object(adf, cfg, category, name, fixed)
                deployed.append(f"{category}/{name}")
    finally:
        # Converge to exactly this run — always, even on a partial deploy failure —
        # so stale objects from prior runs never accumulate (16 -> 32 -> 48). Prune
        # must never mask the deploy error nor crash the deploy: record its own
        # failure on last_prune_error so a non-converging factory is VISIBLE instead
        # of silently piling up.
        deploy_artifacts.last_prune_error = None  # type: ignore[attr-defined]
        deploy_artifacts.last_stale = []  # type: ignore[attr-defined]
        if _prune_enabled():
            try:
                deploy_artifacts.last_pruned = _real_prune(adf, cfg, keep)  # type: ignore[attr-defined]
            except Exception as exc:  # noqa: BLE001 — surface, don't mask/raise
                deploy_artifacts.last_prune_error = f"{type(exc).__name__}: {exc}"  # type: ignore[attr-defined]
        else:
            # Never delete on the agent's own authority — REPORT what a prune
            # would remove so the user can clean up (or set PRUNE_STALE=true).
            try:
                deploy_artifacts.last_stale = _stale_names(adf, cfg, keep)  # type: ignore[attr-defined]
            except Exception as exc:  # noqa: BLE001
                deploy_artifacts.last_prune_error = f"{type(exc).__name__}: {exc}"  # type: ignore[attr-defined]
        # Verify: list what the factory really contains now, per category.
        deploy_artifacts.last_factory_counts = _factory_counts_real(adf, cfg)  # type: ignore[attr-defined]
    return deployed


# Most recent deploy-time heals + prunes + verification (populated by deploy_artifacts).
deploy_artifacts.last_heals = []  # type: ignore[attr-defined]
deploy_artifacts.last_pruned = []  # type: ignore[attr-defined]
deploy_artifacts.last_stale = []  # type: ignore[attr-defined]
deploy_artifacts.last_prune_error = None  # type: ignore[attr-defined]
deploy_artifacts.last_intended_counts = {}  # type: ignore[attr-defined]
deploy_artifacts.last_factory_counts = None  # type: ignore[attr-defined]


def deploy_object(run_dir: str, category: str, name: str, cfg: "AzureConfig | None" = None):
    """Deploy a single named artifact (used by the optional self-heal redeploy)."""
    objects = _load_category(run_dir, category)
    if name not in objects:
        raise KeyError(f"Artifact {category}/{name} not found in {run_dir}")
    if mock_mode():
        return _mock_deploy(run_dir)
    cfg = cfg or AzureConfig.from_env()
    from config import get_datafactory_client

    adf = get_datafactory_client(cfg)
    return _real_deploy_object(adf, cfg, category, name, objects[name])
