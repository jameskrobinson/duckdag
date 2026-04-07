from __future__ import annotations

import pandas as pd
import pytest

from pipeline_core.intermediate import InMemoryStore, IntermediateStore


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------

def test_in_memory_store_satisfies_protocol():
    assert isinstance(InMemoryStore(), IntermediateStore)


# ---------------------------------------------------------------------------
# put / get / has
# ---------------------------------------------------------------------------

def test_put_and_get_roundtrip():
    store = InMemoryStore()
    df = pd.DataFrame({"x": [1, 2, 3]})
    store.put("sources.raw", df)
    result = store.get("sources.raw")
    pd.testing.assert_frame_equal(result, df)


def test_has_returns_true_after_put():
    store = InMemoryStore()
    store.put("t.out", pd.DataFrame({"a": [1]}))
    assert store.has("t.out") is True


def test_has_returns_false_for_missing():
    store = InMemoryStore()
    assert store.has("nonexistent") is False


def test_get_raises_key_error_for_missing():
    store = InMemoryStore()
    with pytest.raises(KeyError, match="nonexistent"):
        store.get("nonexistent")


def test_put_overwrites_existing():
    store = InMemoryStore()
    store.put("t.x", pd.DataFrame({"v": [1]}))
    store.put("t.x", pd.DataFrame({"v": [99]}))
    assert store.get("t.x")["v"].iloc[0] == 99


# ---------------------------------------------------------------------------
# __len__ and __contains__
# ---------------------------------------------------------------------------

def test_len():
    store = InMemoryStore()
    assert len(store) == 0
    store.put("a", pd.DataFrame())
    store.put("b", pd.DataFrame())
    assert len(store) == 2


def test_contains():
    store = InMemoryStore()
    store.put("t.raw", pd.DataFrame())
    assert "t.raw" in store
    assert "t.missing" not in store
