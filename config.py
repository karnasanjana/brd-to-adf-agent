"""Configuration + lazy client construction.

Nothing here touches the network at import time, so ``import config`` is safe
in the offline smoke test. The Foundry/Claude model and the Azure clients are
built only when first requested.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _load_dotenv(path: str = ".env") -> None:
    """Load KEY=VALUE pairs from a local .env into os.environ.

    Tiny, dependency-free, and non-destructive: a variable already set in the
    real environment (e.g. injected by Docker or the shell) always wins.
    """
    if not os.path.isfile(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip().strip('"').strip("'")
            # Skip empty values — setting e.g. AZURE_CLIENT_ID="" breaks
            # DefaultAzureCredential's discovery chain.
            if key and value and key not in os.environ:
                os.environ[key] = value


_load_dotenv()

# Self-heal retry cap (Validator).
RETRY_CAP = 3


def mock_mode() -> bool:
    """When MOCK=true, Azure writes go to a local mock_tenant/ folder and the
    pipeline run is simulated. OFF by default — real Azure is the primary path."""
    return os.environ.get("MOCK", "").strip().lower() in {"1", "true", "yes", "on"}


# Local "tenant" used only in MOCK mode.
MOCK_TENANT_DIR = os.environ.get("MOCK_TENANT_DIR", "mock_tenant")


# --------------------------------------------------------------------------- #
# Foundry / Claude
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class FoundryConfig:
    api_key: str
    model: str
    resource: "str | None" = None     # e.g. 'agent-pipeline-resource'
    base_url: "str | None" = None      # full Anthropic-compatible base URL
    api_version: "str | None" = None

    @classmethod
    def from_env(cls) -> "FoundryConfig":
        api_key = os.environ.get("FOUNDRY_API_KEY")
        base_url = os.environ.get("FOUNDRY_BASE_URL")
        resource = os.environ.get("FOUNDRY_RESOURCE")
        model = os.environ.get("CLAUDE_MODEL", "claude-opus-4-8")
        api_version = os.environ.get("FOUNDRY_API_VERSION")

        # Need an endpoint (base_url or resource). The key is required only for
        # api-key auth; Entra ID auth (az login / Managed Identity) needs no key.
        if not (base_url or resource):
            raise RuntimeError(
                "Set FOUNDRY_BASE_URL (or FOUNDRY_RESOURCE) to call Claude via "
                "Azure AI Foundry."
            )

        # Derive the Foundry resource name from a *.services.ai.azure.com host so
        # users can paste the endpoint shown in the portal without trimming it.
        if resource is None and base_url:
            from urllib.parse import urlparse

            host = urlparse(base_url).netloc
            if host.endswith(".services.ai.azure.com"):
                resource = host.split(".")[0]
        return cls(
            api_key=api_key,
            model=model,
            resource=resource,
            base_url=base_url,
            api_version=api_version,
        )


def _foundry_auth_mode() -> str:
    """'entra' (Microsoft Entra ID / az login / Managed Identity) or 'key'.

    Azure AI Foundry projects authenticate model inference with Entra ID by
    default; the 'project API key' does NOT authorize the /anthropic data plane.
    Default to Entra when FOUNDRY_API_KEY is absent; force with FOUNDRY_AUTH.
    """
    mode = os.environ.get("FOUNDRY_AUTH", "").strip().lower()
    if mode in {"entra", "aad", "azure"}:
        return "entra"
    if mode in {"key", "apikey", "api_key"}:
        return "key"
    return "key" if os.environ.get("FOUNDRY_API_KEY") else "entra"


def _entra_token_provider():
    """Async bearer-token provider over DefaultAzureCredential for the
    Cognitive Services data plane. One credential instance, refreshed per call
    (the SDK caches the token until near expiry)."""
    from azure.identity.aio import DefaultAzureCredential

    scope = os.environ.get(
        "FOUNDRY_SCOPE", "https://cognitiveservices.azure.com/.default"
    )
    credential = DefaultAzureCredential()

    async def provider() -> str:
        token = await credential.get_token(scope)
        return token.token

    return provider


def _anthropic_base_url(cfg: "FoundryConfig") -> str:
    """The Anthropic-compatible endpoint root (the SDK appends /v1/messages).

    For an Azure AI Foundry resource this is https://<resource>.services.ai.
    azure.com/anthropic. If a full base_url was given that already targets a
    non-Azure endpoint, it is used as-is.
    """
    if cfg.resource:
        return f"https://{cfg.resource}.services.ai.azure.com/anthropic"
    return (cfg.base_url or "").rstrip("/")


def _build_anthropic_client(cfg: "FoundryConfig"):
    """Return an async Anthropic client wired for the configured endpoint.

    * Entra ID auth -> AsyncAnthropicFoundry(resource=...) with a token provider.
    * API-key auth  -> plain AsyncAnthropic(base_url=.../anthropic), which sends
      the ``x-api-key`` header the Foundry /anthropic route expects.
    """
    default_query = {"api-version": cfg.api_version} if cfg.api_version else None

    if _foundry_auth_mode() == "entra" and cfg.resource:
        from anthropic import AsyncAnthropicFoundry

        kwargs: dict = {
            "resource": cfg.resource,
            "azure_ad_token_provider": _entra_token_provider(),
        }
        if default_query:
            kwargs["default_query"] = default_query
        return AsyncAnthropicFoundry(**kwargs)

    from anthropic import AsyncAnthropic

    kwargs = {"base_url": _anthropic_base_url(cfg), "api_key": cfg.api_key}
    if default_query:
        kwargs["default_query"] = default_query
    return AsyncAnthropic(**kwargs)


def _anthropic_model():
    """Pydantic AI model backed by Claude on Azure AI Foundry (Anthropic endpoint)."""
    from pydantic_ai.models.anthropic import AnthropicModel
    from pydantic_ai.providers.anthropic import AnthropicProvider

    cfg = FoundryConfig.from_env()
    client = _build_anthropic_client(cfg)
    return AnthropicModel(cfg.model, provider=AnthropicProvider(anthropic_client=client))


def _openai_model():
    """Pydantic AI model backed by an OpenAI-compatible endpoint. Reads OPENAI_API_KEY.

    Covers plain OpenAI, Azure OpenAI, and Azure AI Foundry models exposed on the
    OpenAI-compatible ``/models`` route (e.g. Phi-4-mini-reasoning, DeepSeek, Mistral,
    gpt-*-mini). For those set:

        LLM_PROVIDER=openai
        LLM_MODEL=<deployment name, e.g. Phi-4-mini-reasoning>
        OPENAI_BASE_URL=https://<resource>.services.ai.azure.com/models
        OPENAI_API_KEY=<foundry key>
        OPENAI_API_VERSION=2024-05-01-preview   # Foundry requires this query param

    Key/endpoint/version come from the environment, never hardcoded."""
    from pydantic_ai.models.openai import OpenAIChatModel

    model_id = os.environ.get("LLM_MODEL", "Phi-4-mini-reasoning")
    base_url = os.environ.get("OPENAI_BASE_URL")
    api_version = os.environ.get("OPENAI_API_VERSION")
    if base_url:
        from pydantic_ai.providers.openai import OpenAIProvider

        # Azure AI Foundry's /models route rejects calls without an api-version query
        # param. When set, build an explicit client that appends it to every request;
        # otherwise fall back to the plain base_url/api_key provider.
        if api_version:
            from openai import AsyncOpenAI

            client = AsyncOpenAI(
                base_url=base_url,
                api_key=os.environ.get("OPENAI_API_KEY"),
                default_query={"api-version": api_version},
            )
            provider = OpenAIProvider(openai_client=client)
        else:
            provider = OpenAIProvider(base_url=base_url, api_key=os.environ.get("OPENAI_API_KEY"))
        return OpenAIChatModel(model_id, provider=provider)
    return OpenAIChatModel(model_id)  # OPENAI_API_KEY picked up from env by the provider


def _gemini_model():
    """Pydantic AI model backed by Google Gemini via the google-genai SDK.

    Set:
        LLM_PROVIDER=gemini
        LLM_MODEL=gemini-3.5-flash        # or any Gemini model id
        GEMINI_API_KEY=<key from https://aistudio.google.com/apikey>

    GOOGLE_API_KEY is accepted as a fallback name. Structured output defaults to
    prompted JSON (see structured_output); override with LLM_OUTPUT_MODE=native
    to use Gemini's response-schema mode.

    The unset-LLM_MODEL default is the `gemini-flash-latest` ALIAS, not a pinned
    id: Google retires pinned models (gemini-2.5-flash started 404ing
    'no longer available' on 2026-07-09 and bricked the plan/heal loops) and the
    alias always tracks a live one. Pin an explicit id in .env when eval
    stability matters more than availability."""
    from google.genai import Client
    from google.genai.types import HttpOptions
    from pydantic_ai.models.google import GoogleModel
    from pydantic_ai.providers.google import GoogleProvider

    model_id = os.environ.get("LLM_MODEL", "gemini-flash-latest")
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "Set GEMINI_API_KEY (or GOOGLE_API_KEY) in .env to use LLM_PROVIDER=gemini."
        )
    # Hard per-request timeout: without one a stalled HTTP call blocks /plan
    # forever (observed 300s+ hangs) and retry.py never gets a chance — it only
    # fires on a returned error. LLM_TIMEOUT is seconds; HttpOptions wants ms.
    timeout_s = float(os.environ.get("LLM_TIMEOUT", "60"))
    client = Client(api_key=api_key, http_options=HttpOptions(timeout=int(timeout_s * 1000)))
    return GoogleModel(model_id, provider=GoogleProvider(client=client))


# provider aliases -> builder
_MODEL_BUILDERS = {
    "anthropic": _anthropic_model,
    "claude": _anthropic_model,
    "foundry": _anthropic_model,
    "openai": _openai_model,
    "azure-openai": _openai_model,
    "azure_openai": _openai_model,
    "google": _gemini_model,
    "gemini": _gemini_model,
}


def get_model():
    """Build a Pydantic AI model for the configured provider — the SINGLE place the
    project is coupled to an LLM. Swap providers with env vars, no code change:

        LLM_PROVIDER=anthropic|openai|gemini   (default: gemini)
        LLM_MODEL=<provider model id>          (provider default if unset)

    Because every agent uses ``output_type=<Pydantic model>`` (structured output),
    any provider with tool/JSON mode drops in. Keys/endpoints come from the
    environment, never hardcoded.
    """
    provider = os.environ.get("LLM_PROVIDER", "gemini").strip().lower()
    builder = _MODEL_BUILDERS.get(provider)
    if builder is None:
        raise RuntimeError(
            f"Unknown LLM_PROVIDER '{provider}'. Supported: "
            f"{', '.join(sorted(set(_MODEL_BUILDERS)))}."
        )
    return builder()


# Backward-compatible alias — existing callers keep working; new code uses get_model().
def get_claude_model():
    return get_model()


_REASONING_HINTS = (
    "gpt-5", "o1", "o3", "o4", "reasoning", "gemini-2.5", "gemini-3",
    # Gemini 'latest' aliases resolve to 2.5+ models, all thinking-by-default.
    "gemini-flash-latest", "gemini-flash-lite-latest", "gemini-pro-latest",
)


def is_reasoning_model() -> bool:
    """True when LLM_MODEL is a reasoning/thinking model (gpt-5*, o1/o3/o4,
    any '*reasoning*', or Gemini 2.5+ which thinks by default). These spend part
    of the output-token budget on hidden reasoning, so they get a bigger
    completion budget and no custom temperature."""
    m = os.environ.get("LLM_MODEL", "").lower()
    return any(h in m for h in _REASONING_HINTS)


def default_model_settings() -> dict:
    """Model settings for the parser/heal agents, adapted to the active model.

    Reasoning models (Gemini 2.5+, o-series) reject ``temperature`` other than the
    default and burn tokens on hidden reasoning — so drop temperature and give a
    bigger completion budget. Everything else keeps deterministic temperature=0.
    """
    if is_reasoning_model():
        return {"max_tokens": 32000}
    return {"max_tokens": 16384, "temperature": 0.0}


def structured_output(model_cls):
    """Wrap a Pydantic output type in the output MODE the active provider supports.

    Pydantic AI's default structured output uses forced tool-calling
    (tool_choice="required"). That fails on chat models whose serving stack has no
    tool-call parser — e.g. Phi-4-mini on Azure Foundry returns
    ``tool_choice="required" requires --tool-call-parser to be set``. For those we
    fall back to PROMPTED JSON (schema in the prompt, model returns JSON text, SDK
    validates) which works on any chat model. Claude keeps tool-calling (best).

    Override per deployment with LLM_OUTPUT_MODE=tool|prompted|native.
    """
    mode = os.environ.get("LLM_OUTPUT_MODE", "").strip().lower()
    if not mode:
        provider = os.environ.get("LLM_PROVIDER", "gemini").strip().lower()
        mode = "tool" if provider in {"anthropic", "claude", "foundry"} else "prompted"
    if mode == "prompted":
        from pydantic_ai import PromptedOutput

        return PromptedOutput(model_cls)
    if mode == "native":
        from pydantic_ai import NativeOutput

        return NativeOutput(model_cls)
    return model_cls  # tool mode = Pydantic AI default


# --------------------------------------------------------------------------- #
# Azure
# --------------------------------------------------------------------------- #


def storage_kind() -> str:
    """'blob' (plain Azure Blob, no hierarchical namespace) or 'adls' (Data Lake
    Gen2, HNS on). Set STORAGE_KIND explicitly, or it's inferred from the account
    URL host (.blob -> blob, .dfs -> adls). Default: adls."""
    k = os.environ.get("STORAGE_KIND", "").strip().lower()
    if k in {"blob", "azureblob", "blobstorage"}:
        return "blob"
    if k in {"adls", "adls_gen2", "gen2", "datalake"}:
        return "adls"
    return "blob" if ".blob." in os.environ.get("ADLS_ACCOUNT_URL", "") else "adls"


def blob_endpoint(account_name: str) -> str:
    return f"https://{account_name}.blob.core.windows.net"


def dfs_endpoint(account_name: str) -> str:
    return f"https://{account_name}.dfs.core.windows.net"


@dataclass(frozen=True)
class AzureConfig:
    subscription_id: str
    resource_group: str
    factory_name: str          # the Data Factory
    adls_account_name: str
    adls_account_url: str      # https://<acct>.dfs(or blob).core.windows.net
    location: str = "eastus"

    @property
    def store_endpoint(self) -> str:
        """The endpoint ADF/the SDK should use, matching storage_kind()."""
        return (
            blob_endpoint(self.adls_account_name)
            if storage_kind() == "blob"
            else dfs_endpoint(self.adls_account_name)
        )

    @classmethod
    def from_env(cls) -> "AzureConfig":
        def need(key: str) -> str:
            val = os.environ.get(key)
            if not val:
                raise RuntimeError(f"Environment variable {key} is required for deploy.")
            return val

        account = need("ADLS_ACCOUNT_NAME")
        default_url = (
            blob_endpoint(account) if storage_kind() == "blob" else dfs_endpoint(account)
        )
        return cls(
            subscription_id=need("AZURE_SUBSCRIPTION_ID"),
            resource_group=need("AZURE_RESOURCE_GROUP"),
            factory_name=need("ADF_FACTORY_NAME"),
            adls_account_name=account,
            adls_account_url=os.environ.get("ADLS_ACCOUNT_URL", default_url),
            location=os.environ.get("AZURE_LOCATION", "eastus"),
        )


def get_credential():
    """DefaultAzureCredential — works with Managed Identity in Azure and with
    az-cli / env credentials locally. No account keys anywhere."""
    from azure.identity import DefaultAzureCredential

    return DefaultAzureCredential()


def get_datafactory_client(cfg: "AzureConfig | None" = None):
    from azure.mgmt.datafactory import DataFactoryManagementClient

    cfg = cfg or AzureConfig.from_env()
    return DataFactoryManagementClient(get_credential(), cfg.subscription_id)


def get_datalake_client(cfg: "AzureConfig | None" = None):
    from azure.storage.filedatalake import DataLakeServiceClient

    cfg = cfg or AzureConfig.from_env()
    # Derive the endpoint from the account name + storage_kind (via store_endpoint)
    # so a stale/mismatched ADLS_ACCOUNT_URL (e.g. a .blob URL left in the field)
    # can never point the Gen2 client at the wrong endpoint. Mirrors the blob client.
    return DataLakeServiceClient(account_url=cfg.store_endpoint, credential=get_credential())


def get_blob_service_client(cfg: "AzureConfig | None" = None):
    """Blob client for plain (non-HNS) Azure Storage. Managed Identity, no keys."""
    from azure.storage.blob import BlobServiceClient

    cfg = cfg or AzureConfig.from_env()
    return BlobServiceClient(account_url=blob_endpoint(cfg.adls_account_name), credential=get_credential())
