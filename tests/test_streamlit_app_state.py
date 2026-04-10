from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("streamlit")

import src.ui.streamlit_app as streamlit_app


def test_sync_batch_dir_widget_from_state_before_render_applies_pending_sync() -> None:
    canonical_path = str(Path("runs") / "batches" / "batch_005")
    state = {
        streamlit_app.BATCH_DIR_STATE_KEY: canonical_path,
        streamlit_app.BATCH_DIR_WIDGET_KEY: "draft",
        streamlit_app.BATCH_DIR_WIDGET_SYNC_PENDING_KEY: True,
    }

    streamlit_app._sync_batch_dir_widget_from_state_before_render(state)

    assert state[streamlit_app.BATCH_DIR_WIDGET_KEY] == canonical_path
    assert state[streamlit_app.BATCH_DIR_WIDGET_SYNC_PENDING_KEY] is False


def test_sync_batch_dir_widget_from_state_before_render_preserves_widget_value_without_pending_sync() -> None:
    state = {
        streamlit_app.BATCH_DIR_STATE_KEY: str(Path("runs") / "batches" / "batch_005"),
        streamlit_app.BATCH_DIR_WIDGET_KEY: "draft",
        streamlit_app.BATCH_DIR_WIDGET_SYNC_PENDING_KEY: False,
    }

    streamlit_app._sync_batch_dir_widget_from_state_before_render(state)

    assert state[streamlit_app.BATCH_DIR_WIDGET_KEY] == "draft"


def test_get_selected_batch_dir_from_widget_prefers_widget_value_then_choice() -> None:
    widget_path = str(Path("runs") / "batches" / "batch_005")
    choice_path = str(Path("runs") / "batches" / "batch_004")

    state = {
        streamlit_app.BATCH_DIR_WIDGET_KEY: f"  {widget_path}  ",
        "ui_batch_dir_choice": choice_path,
    }
    assert streamlit_app._get_selected_batch_dir_from_widget(state) == widget_path

    state[streamlit_app.BATCH_DIR_WIDGET_KEY] = "   "
    assert streamlit_app._get_selected_batch_dir_from_widget(state) == choice_path


def test_store_loaded_batch_state_updates_canonical_path_without_touching_widget_value(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(streamlit_app, "REPO_ROOT", tmp_path)
    bundle = SimpleNamespace(
        batch_dir=tmp_path / "runs" / "batches" / "batch_005",
        seed_table_rows=[{"batch_index": 7}],
    )
    state = {
        streamlit_app.BATCH_DIR_STATE_KEY: "",
        streamlit_app.BATCH_DIR_WIDGET_KEY: "draft",
        streamlit_app.BATCH_DIR_WIDGET_SYNC_PENDING_KEY: False,
        "ui_selected_seed_index": None,
    }

    streamlit_app._store_loaded_batch_state(bundle, state)

    assert state["ui_loaded_batch"] is bundle
    assert state[streamlit_app.BATCH_DIR_STATE_KEY] == str(Path("runs") / "batches" / "batch_005")
    assert state[streamlit_app.BATCH_DIR_WIDGET_KEY] == "draft"
    assert state[streamlit_app.BATCH_DIR_WIDGET_SYNC_PENDING_KEY] is True
    assert state["ui_selected_seed_index"] == 7


def test_apply_pending_run_batch_prefills_before_render_updates_run_batch_state() -> None:
    state = {
        "ui_run_prefill_payload": {
            "theory_config_path": "runs/accepted_baselines/accepted_baseline_001/accepted_theory_snapshot.yaml",
            "seeds_csv_path": "data/benchmarks/seeds.csv",
            "max_references": 10,
            "max_related": 9,
            "max_hard_negatives": 8,
            "top_k": 7,
            "label_source": "silver",
            "refresh": True,
        },
        streamlit_app.THEORY_CONFIG_STATE_KEY: "",
        streamlit_app.THEORY_CONFIG_WIDGET_KEY: "draft",
        streamlit_app.THEORY_CONFIG_WIDGET_SYNC_PENDING_KEY: False,
    }

    streamlit_app._apply_pending_run_batch_prefills_before_render(state)

    assert state[streamlit_app.THEORY_CONFIG_STATE_KEY].endswith("accepted_theory_snapshot.yaml")
    assert state[streamlit_app.THEORY_CONFIG_WIDGET_SYNC_PENDING_KEY] is True
    assert state["ui_seeds_csv_path"] == "data/benchmarks/seeds.csv"
    assert state["ui_run_max_references"] == 10
    assert state["ui_run_refresh"] is True
    assert state["ui_run_prefill_payload"] is None


def test_clear_selected_preset_helpers_only_clear_selection_state() -> None:
    state = {
        "ui_selected_benchmark_preset_id": "benchmark_smoke_001",
        "ui_selected_benchmark_preset_path": "configs/presets/benchmarks/benchmark_smoke_001.json",
        "ui_selected_benchmark_preset_seeds_csv": "data/benchmarks/seeds.csv",
        "ui_selected_benchmark_preset_warning": "warn",
        "ui_selected_eval_preset_id": "eval_micro_001",
        "ui_selected_eval_preset_path": "configs/presets/evals/eval_micro_001.json",
        "ui_selected_eval_preset_values": {"max_references": 10},
        "ui_selected_eval_preset_warning": "warn",
        "ui_seeds_csv_path": "data/benchmarks/custom.csv",
        "ui_run_max_references": 5,
    }

    streamlit_app._clear_selected_benchmark_preset(state)
    streamlit_app._clear_selected_evaluation_preset(state)

    assert state["ui_selected_benchmark_preset_id"] == ""
    assert state["ui_selected_benchmark_preset_seeds_csv"] == ""
    assert state["ui_selected_eval_preset_id"] == ""
    assert state["ui_selected_eval_preset_values"] is None
    assert state["ui_seeds_csv_path"] == "data/benchmarks/custom.csv"
    assert state["ui_run_max_references"] == 5
