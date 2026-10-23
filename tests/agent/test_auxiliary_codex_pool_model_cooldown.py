"""A per-model credential bench must not disable the Codex auxiliary route for other models.

Observed on the Hermes VPS 2026-10-22 (Photon DM session ``20261001_185158_5f8dc2d4``): the sole
``openai-codex`` pool entry carried ``model_cooldowns: {"gpt-5.4": <now + 365d>}`` — the
ChatGPT-plan entitlement bench (``MODEL_ENTITLEMENT_BENCH_SECONDS``). ``_build_codex_client``
knew the model it was resolving but selected the pool entry model-lessly, and
``model_cooldown_until`` reads a model-less request as "any active cooldown blocks", so every
auxiliary Codex call resolved no token while the main lane (which passes its model) kept
working. Compression then failed on every turn with ``resolve_provider_client: openai-codex
requested but no Codex OAuth token found``.
"""

from __future__ import annotations

import time

import pytest

import agent.auxiliary_client as aux
from agent.credential_pool_model_cooldowns import MODEL_ENTITLEMENT_BENCH_SECONDS
from hermes_cli.auth import write_credential_pool

BENCHED = "gpt-5.4"
WORKING = "gpt-5.6-luna"


@pytest.fixture
def codex_pool(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
    monkeypatch.setattr(aux, "_client_cache", {})
    write_credential_pool(
        "openai-codex",
        [
            {
                "id": "924302",
                "label": "openai-codex-oauth-2",
                "auth_type": "oauth",
                "priority": 0,
                "source": "manual:device_code",
                "access_token": "tok-codex",
                "refresh_token": "rt-codex",
                "base_url": "https://chatgpt.com/backend-api/codex",
                "model_cooldowns": {BENCHED: time.time() + MODEL_ENTITLEMENT_BENCH_SECONDS},
            }
        ],
    )
    return tmp_path / "hermes"


def test_model_less_selection_is_refused_while_one_model_is_benched(codex_pool):
    """Control: the pool's model-less contract declines the entry (unchanged by this fix)."""
    present, entry = aux._select_pool_entry("openai-codex")
    assert present is True
    assert entry is None


def test_named_model_selection_ignores_a_sibling_models_bench(codex_pool):
    present, entry = aux._select_pool_entry("openai-codex", model=WORKING)
    assert present is True
    assert entry is not None
    assert aux._pool_runtime_api_key(entry) == "tok-codex"


def test_aux_codex_client_resolves_for_an_unbenched_model(codex_pool):
    client, model = aux.resolve_provider_client("openai-codex", WORKING)
    assert client is not None, "auxiliary Codex resolution must survive a sibling model's bench"
    assert model == WORKING


def test_aux_codex_client_still_refuses_the_benched_model(codex_pool):
    """The bench keeps doing its job: the benched model itself resolves nothing."""
    client, _ = aux.resolve_provider_client("openai-codex", BENCHED)
    assert client is None
