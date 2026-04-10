from __future__ import annotations

from pathlib import Path
from typing import Any, MutableMapping

import streamlit as st

from src.config import REPO_ROOT
from src.eval.benchmark import (
    DEFAULT_BATCH_MAX_HARD_NEGATIVES,
    DEFAULT_BATCH_MAX_REFERENCES,
    DEFAULT_BATCH_MAX_RELATED,
    DEFAULT_BATCH_TOP_K,
)
from src.ui.diagnostics import (
    available_numeric_metrics,
    best_and_worst_rows,
    choose_default_primary_metric,
    choose_default_scatter_metrics,
    choose_ranking_metric,
    filter_diagnostic_rows,
    metric_values,
    scatter_points,
)
from src.ui.benchmark_health import (
    BenchmarkAuditError,
    BenchmarkAuditExportResult,
    ComparisonDiscriminativenessAssessment,
    PrimaryBatchHealthAssessment,
    build_benchmark_audit_export_request,
    build_seed_quality_rows,
    evaluate_comparison_discriminativeness,
    evaluate_primary_batch_health,
    export_benchmark_audit,
)
from src.ui.benchmark_curation import (
    CURATION_DECISION_FILTER_OPTIONS,
    CURATION_DECISION_OPTIONS,
    BenchmarkCurationError,
    BenchmarkCurationExportResult,
    build_benchmark_curation_export_request,
    build_seed_curation_rows,
    export_benchmark_curation,
    filter_curation_rows,
    normalize_curation_decisions,
    summarize_curation_decisions,
)
from src.ui.cohort_analysis import (
    CohortPairwiseAnalysisRow,
    build_pairwise_analysis_detail,
    build_pairwise_analysis_rows,
    build_pairwise_analysis_table_rows,
    choose_default_cohort_analysis_metric,
    choose_default_reference_batch_id,
    filter_pairwise_analysis_rows,
    find_pairwise_analysis_row,
    pairwise_metric_availability_counts,
    sort_pairwise_analysis_rows,
)
from src.ui.cohort_study import (
    COHORT_STUDY_DECISION_FILTER_OPTIONS,
    COHORT_STUDY_DECISION_OPTIONS,
    CohortStudyError,
    CohortStudyExportResult,
    build_candidate_decision_rows,
    build_cohort_study_context_key,
    build_cohort_study_export_request,
    build_cohort_study_table_rows,
    export_cohort_study,
    filter_cohort_study_rows,
    normalize_cohort_study_decisions,
    suggest_cohort_study_decision,
    summarize_cohort_study_decisions,
)
from src.ui.cohort_study_registry import (
    COHORT_STUDY_REGISTRY_DECISION_FILTER_OPTIONS,
    CohortStudyCandidateRow,
    CohortStudyRegistryEntry,
    build_cohort_study_candidate_detail,
    build_cohort_study_candidate_rows,
    build_cohort_study_candidate_table_rows,
    build_cohort_study_detail,
    build_cohort_study_registry_rows,
    choose_default_cohort_study_id,
    filter_cohort_study_candidate_rows,
    find_cohort_study_candidate_row,
    find_cohort_study_entry,
    scan_cohort_studies,
)
from src.ui.workspace_inbox import (
    WORKSPACE_INBOX_QUEUE_LABELS,
    WORKSPACE_INBOX_QUEUE_TYPES,
    WorkspaceInboxItem,
    build_workspace_inbox_detail,
    build_workspace_inbox_table_rows,
    choose_default_workspace_inbox_item_key,
    filter_workspace_inbox_items,
    find_workspace_inbox_item,
    group_workspace_inbox_items,
    scan_workspace_inbox,
)
from src.ui.experiment_matrix import (
    UNCLASSIFIED_COHORT_KEY,
    ExperimentBatchRow,
    available_experiment_statuses,
    build_cohort_rows,
    build_experiment_detail,
    build_experiment_table_rows,
    choose_default_leaderboard_metric,
    filter_experiment_rows,
    find_experiment_row,
    group_experiment_cohorts,
    scan_experiment_batches,
    sort_experiment_rows,
)
from src.ui.curated_benchmark_preset import (
    CuratedBenchmarkPresetError,
    build_curated_benchmark_preset_save_request,
    load_curation_bundle_context,
    save_curated_benchmark_preset,
)
from src.ui.batch_loader import BatchLoadError, BatchUiBundle, discover_batch_dirs, load_batch_bundle
from src.ui.batch_runner import (
    SUPPORTED_LABEL_SOURCES,
    BatchRunSummary,
    BatchRunValidationError,
    build_batch_run_request,
    run_batch_request,
)
from src.ui.decision_guardrails import (
    DecisionGuardrailAssessment,
    evaluate_decision_guardrails,
    requires_explicit_promotion_override,
)
from src.ui.baseline_registry import (
    AcceptedBaselineEntry,
    build_accepted_baseline_detail,
    build_accepted_baseline_registry_rows,
    choose_default_accepted_baseline_id,
    find_accepted_baseline_entry,
    scan_accepted_baselines,
)
from src.ui.preset_registry import (
    BenchmarkPresetEntry,
    EvaluationPresetEntry,
    PresetRegistryError,
    build_benchmark_preset_detail,
    build_benchmark_preset_rows,
    build_benchmark_preset_save_request,
    build_benchmark_run_batch_values,
    build_evaluation_preset_detail,
    build_evaluation_preset_rows,
    build_evaluation_preset_save_request,
    build_evaluation_run_batch_values,
    choose_default_benchmark_preset_id,
    choose_default_evaluation_preset_id,
    find_benchmark_preset_entry,
    find_evaluation_preset_entry,
    save_benchmark_preset,
    save_evaluation_preset,
    scan_benchmark_presets,
    scan_evaluation_presets,
)
from src.ui.launch_profile_registry import (
    LaunchProfileEntry,
    LaunchProfileRegistryError,
    build_launch_profile_detail,
    build_launch_profile_rows,
    build_launch_profile_run_batch_values,
    build_launch_profile_save_request,
    choose_default_launch_profile_id,
    find_launch_profile_entry,
    save_launch_profile,
    scan_launch_profiles,
)
from src.ui.provenance_timeline import (
    TimelineEntry,
    available_timeline_statuses,
    build_timeline_detail,
    build_timeline_rows,
    choose_default_timeline_entry_key,
    filter_timeline_entries,
    find_timeline_entry,
    scan_provenance_timeline,
)
from src.ui.report_bundle_export import (
    EXPECTED_REPORT_ARTIFACT_TYPES,
    ReportArtifactContext,
    ReportBundleExportError,
    ReportBundleExportResult,
    build_accepted_baseline_report_context,
    build_batch_report_context,
    build_candidate_run_report_context,
    build_comparison_report_context,
    build_preset_report_context,
    build_reeval_outcome_report_context,
    build_report_export_request,
    build_review_packet_report_context,
    export_report_bundle,
)
from src.ui.run_context import (
    RunContextError,
    build_run_context_payload_from_request,
    write_run_context,
)
from src.ui.baseline_promotion import (
    BaselinePromotionError,
    BaselinePromotionResult,
    OutcomeContext,
    build_baseline_promotion_request,
    load_outcome_context,
    promotion_compatibility_errors,
    save_accepted_baseline_artifacts,
)
from src.ui.comparison import (
    COMPARISON_STATUS_OPTIONS,
    align_common_seed_rows,
    choose_default_comparison_metric,
    common_completed_seed_count,
    common_numeric_metrics,
    comparison_metric_summary,
    compatibility_warnings,
    metric_overlap_counts,
    paired_metric_rows,
)
from src.ui.comparison_export import (
    DECISION_STATUS_OPTIONS,
    ComparisonExportError,
    ComparisonSaveResult,
    build_comparison_save_request,
    save_comparison_artifacts,
)
from src.ui.candidate_apply_run import (
    CandidateApplyRunError,
    CandidateApplyRunResult,
    build_candidate_apply_run_request,
    packet_primary_compatibility_errors,
    run_candidate_apply_and_batch,
)
from src.ui.review_packet_export import (
    ReviewPacketExportError,
    ReviewPacketExportResult,
    build_review_packet_export_request,
    save_review_packet_artifacts,
)
from src.ui.study_provenance import (
    StudySourceContextAssessment,
    build_study_source_artifact_fields,
    build_study_source_session_payload,
    evaluate_study_source_context,
    load_study_source_from_json_file,
)
from src.ui.reeval_outcome_export import (
    CandidateRunContext,
    ReevalOutcomeExportError,
    ReevalOutcomeSaveResult,
    build_reeval_outcome_save_request,
    candidate_run_compatibility_errors,
    load_candidate_run_context,
    save_reeval_outcome_artifacts,
)
from src.ui.reply_preview import (
    ReplyPreviewResult,
    ReviewPacketBundle,
    ReviewPacketLoadError,
    load_review_packet_bundle,
    preview_candidate_reply,
)
from src.ui.seed_detail import (
    SEED_DETAIL_SUBSETS,
    build_seed_detail_sections,
    choose_default_seed_doi,
    find_seed_row_by_doi,
    select_seed_rows_for_subset,
)


SESSION_DEFAULTS = {
    "ui_batch_dir_input": "",
    "_ui_batch_dir_input": "",
    "ui_batch_dir_choice": "",
    "ui_batch_dir_input_sync_pending": False,
    "ui_loaded_batch": None,
    "ui_batch_load_error": None,
    "ui_seed_status_mode": "all",
    "ui_seed_doi_filter": "",
    "ui_seed_sort_metric": "batch_index",
    "ui_seed_sort_desc": False,
    "ui_selected_seed_index": None,
    "ui_seed_detail_subset": "all",
    "ui_seed_detail_doi": "",
    "ui_diag_status_mode": "completed only",
    "ui_diag_primary_metric": "",
    "ui_diag_x_metric": "",
    "ui_diag_y_metric": "",
    "ui_diag_limit": 5,
    "ui_comparison_secondary_dir_input": "",
    "ui_comparison_secondary_bundle": None,
    "ui_comparison_error": None,
    "ui_comparison_status_mode": COMPARISON_STATUS_OPTIONS[0],
    "ui_comparison_metric": "",
    "ui_comparison_limit": 5,
    "ui_comparison_doi_filter": "",
    "ui_comparison_save_id": "",
    "ui_comparison_reviewer": "",
    "ui_comparison_decision_status": DECISION_STATUS_OPTIONS[2],
    "ui_comparison_notes": "",
    "ui_comparison_save_error": None,
    "ui_comparison_save_result": None,
    "ui_review_packet_id": "",
    "ui_review_packet_comparison_id": "",
    "ui_review_packet_reviewer": "",
    "ui_review_packet_metric": "",
    "ui_review_packet_max_regressions": 10,
    "ui_review_packet_max_improvements": 10,
    "ui_review_packet_error": None,
    "ui_review_packet_result": None,
    "ui_reply_packet_dir_input": "",
    "ui_loaded_reply_packet": None,
    "ui_reply_packet_error": None,
    "ui_candidate_reply_path": "",
    "ui_reply_preview_error": None,
    "ui_reply_preview_result": None,
    "ui_candidate_apply_id": "",
    "ui_candidate_output_batch_id": "",
    "ui_candidate_apply_reviewer": "",
    "ui_candidate_apply_notes": "",
    "ui_candidate_apply_error": None,
    "ui_candidate_apply_warning": None,
    "ui_candidate_apply_result": None,
    "ui_candidate_apply_in_progress": False,
    "ui_reeval_candidate_run_dir": "",
    "ui_reeval_outcome_id": "",
    "ui_reeval_reviewer": "",
    "ui_reeval_decision_status": DECISION_STATUS_OPTIONS[2],
    "ui_reeval_notes": "",
    "ui_reeval_metric": "",
    "ui_reeval_error": None,
    "ui_reeval_result": None,
    "ui_promotion_candidate_run_dir": "",
    "ui_promotion_outcome_dir": "",
    "ui_promotion_baseline_id": "",
    "ui_promotion_reviewer": "",
    "ui_promotion_notes": "",
    "ui_promotion_override_used": False,
    "ui_promotion_override_reason": "",
    "ui_promotion_error": None,
    "ui_promotion_result": None,
    "ui_initial_doi_context": "",
    "ui_theory_config_path": "",
    "_ui_theory_config_path": "",
    "ui_theory_config_path_sync_pending": False,
    "ui_seeds_csv_path": "data/benchmarks/seeds.csv",
    "ui_run_batch_id": "",
    "ui_run_max_references": DEFAULT_BATCH_MAX_REFERENCES,
    "ui_run_max_related": DEFAULT_BATCH_MAX_RELATED,
    "ui_run_max_hard_negatives": DEFAULT_BATCH_MAX_HARD_NEGATIVES,
    "ui_run_top_k": DEFAULT_BATCH_TOP_K,
    "ui_run_label_source": SUPPORTED_LABEL_SOURCES[0],
    "ui_run_refresh": False,
    "ui_run_in_progress": False,
    "ui_last_run_summary": None,
    "ui_last_run_error": None,
    "ui_last_run_warning": None,
    "ui_partial_run_bundle": None,
    "ui_partial_run_summary": None,
    "ui_selected_accepted_baseline_id": "",
    "ui_selected_accepted_baseline_dir": "",
    "ui_selected_accepted_baseline_theory_path": "",
    "ui_registry_selected_baseline_id": "",
    "ui_selected_benchmark_preset_id": "",
    "ui_selected_benchmark_preset_path": "",
    "ui_selected_benchmark_preset_seeds_csv": "",
    "ui_selected_benchmark_preset_warning": None,
    "ui_selected_eval_preset_id": "",
    "ui_selected_eval_preset_path": "",
    "ui_selected_eval_preset_values": None,
    "ui_selected_eval_preset_warning": None,
    "ui_registry_selected_benchmark_preset_id": "",
    "ui_registry_selected_eval_preset_id": "",
    "ui_new_benchmark_preset_id": "",
    "ui_new_benchmark_preset_seeds_csv": "data/benchmarks/seeds.csv",
    "ui_new_benchmark_preset_description": "",
    "ui_new_benchmark_preset_tags": "",
    "ui_benchmark_preset_error": None,
    "ui_benchmark_preset_saved_path": None,
    "ui_new_eval_preset_id": "",
    "ui_new_eval_max_references": DEFAULT_BATCH_MAX_REFERENCES,
    "ui_new_eval_max_related": DEFAULT_BATCH_MAX_RELATED,
    "ui_new_eval_max_hard_negatives": DEFAULT_BATCH_MAX_HARD_NEGATIVES,
    "ui_new_eval_top_k": DEFAULT_BATCH_TOP_K,
    "ui_new_eval_label_source": SUPPORTED_LABEL_SOURCES[0],
    "ui_new_eval_refresh": False,
    "ui_new_eval_preset_description": "",
    "ui_eval_preset_error": None,
    "ui_eval_preset_saved_path": None,
    "ui_run_prefill_payload": None,
    "ui_selected_launch_profile_id": "",
    "ui_selected_launch_profile_path": "",
    "ui_selected_launch_profile_values": None,
    "ui_selected_launch_profile_warning": None,
    "ui_registry_selected_launch_profile_id": "",
    "ui_new_launch_profile_id": "",
    "ui_new_launch_profile_description": "",
    "ui_new_launch_profile_tags": "",
    "ui_launch_profile_error": None,
    "ui_launch_profile_saved_path": None,
    "ui_launch_profile_run_batch_id": "",
    "ui_launch_profile_run_initial_doi_context": "",
    "ui_launch_profile_run_error": None,
    "ui_launch_profile_run_warning": None,
    "ui_launch_profile_run_summary": None,
    "ui_experiments_selected_cohort_key": "",
    "ui_experiments_same_as_primary_only": False,
    "ui_experiments_statuses": [],
    "ui_experiments_search_text": "",
    "ui_experiments_metric": "",
    "ui_experiments_selected_batch_id": "",
    "ui_cohort_analysis_selected_cohort_key": "",
    "ui_cohort_analysis_same_as_primary_only": False,
    "ui_cohort_analysis_reference_batch_id": "",
    "ui_cohort_analysis_metric": "",
    "ui_cohort_analysis_include_partial_failed": False,
    "ui_cohort_analysis_search_text": "",
    "ui_cohort_analysis_selected_candidate_batch_id": "",
    "ui_cohort_study_context_key": "",
    "ui_cohort_study_decisions": None,
    "ui_cohort_study_decision_filter": COHORT_STUDY_DECISION_FILTER_OPTIONS[0],
    "ui_cohort_study_only_usable": False,
    "ui_cohort_study_only_unusable": False,
    "ui_cohort_study_search_text": "",
    "ui_cohort_study_selected_candidate_batch_id": "",
    "ui_cohort_study_id": "",
    "ui_cohort_study_reviewer": "",
    "ui_cohort_study_notes": "",
    "ui_cohort_study_include_markdown_summary": True,
    "ui_cohort_study_include_shortlist_csv": True,
    "ui_cohort_study_error": None,
    "ui_cohort_study_result": None,
    "ui_study_registry_selected_study_id": "",
    "ui_study_registry_decision_filter": COHORT_STUDY_REGISTRY_DECISION_FILTER_OPTIONS[0],
    "ui_study_registry_usable_only": False,
    "ui_study_registry_search_text": "",
    "ui_study_registry_selected_candidate_batch_id": "",
    "ui_study_source_context": None,
    "ui_inbox_queue_types": list(WORKSPACE_INBOX_QUEUE_TYPES),
    "ui_inbox_search_text": "",
    "ui_inbox_recent_limit": 10,
    "ui_inbox_feedback": None,
    "ui_inbox_error": None,
    "ui_history_artifact_types": [],
    "ui_history_statuses": [],
    "ui_history_search_text": "",
    "ui_history_selected_entry_key": "",
    "ui_report_id": "",
    "ui_report_reviewer": "",
    "ui_report_notes": "",
    "ui_report_include_raw_artifacts": True,
    "ui_report_include_markdown_summary": True,
    "ui_report_comparison_dir": "",
    "ui_report_packet_dir": "",
    "ui_report_candidate_run_dir": "",
    "ui_report_outcome_dir": "",
    "ui_report_baseline_dir": "",
    "ui_report_error": None,
    "ui_report_result": None,
    "ui_benchmark_audit_id": "",
    "ui_benchmark_audit_reviewer": "",
    "ui_benchmark_audit_notes": "",
    "ui_benchmark_audit_include_markdown_summary": True,
    "ui_benchmark_audit_error": None,
    "ui_benchmark_audit_result": None,
    "ui_curation_source_batch_id": "",
    "ui_curation_decisions": None,
    "ui_curation_decision_filter": CURATION_DECISION_FILTER_OPTIONS[0],
    "ui_curation_only_failed": False,
    "ui_curation_only_saturated": False,
    "ui_curation_only_tie_like": False,
    "ui_curation_doi_filter": "",
    "ui_curation_selected_dois": [],
    "ui_curation_target_decision": CURATION_DECISION_OPTIONS[1],
    "ui_curation_id": "",
    "ui_curation_reviewer": "",
    "ui_curation_notes": "",
    "ui_curation_export_only_kept": True,
    "ui_curation_include_review_csv": True,
    "ui_curation_include_markdown_summary": True,
    "ui_curation_error": None,
    "ui_curation_result": None,
    "ui_curation_preset_id": "",
    "ui_curation_preset_dir": "",
    "ui_curation_preset_description": "",
    "ui_curation_preset_tags": "",
    "ui_curation_preset_select_for_run_batch": False,
    "ui_curation_preset_error": None,
    "ui_curation_preset_saved_path": None,
}
BATCH_DIR_STATE_KEY = "ui_batch_dir_input"
BATCH_DIR_WIDGET_KEY = "_ui_batch_dir_input"
BATCH_DIR_WIDGET_SYNC_PENDING_KEY = "ui_batch_dir_input_sync_pending"
THEORY_CONFIG_STATE_KEY = "ui_theory_config_path"
THEORY_CONFIG_WIDGET_KEY = "_ui_theory_config_path"
THEORY_CONFIG_WIDGET_SYNC_PENDING_KEY = "ui_theory_config_path_sync_pending"
METRIC_LABELS = {
    "precision_at_k": "Precision@k",
    "recall_at_k": "Recall@k",
    "ndcg_at_k": "nDCG@k",
    "brier_score": "Brier Score",
    "expected_calibration_error": "Expected Calibration Error",
}
SEED_TABLE_COLUMNS = [
    "batch_index",
    "doi",
    "status",
    "precision_at_k",
    "recall_at_k",
    "ndcg_at_k",
    "brier_score",
    "expected_calibration_error",
    "failed_stage",
    "error_type",
]


def main() -> None:
    st.set_page_config(
        page_title="Scholarly Similarity Batch Viewer",
        page_icon=":bar_chart:",
        layout="wide",
    )
    _initialize_session_state()

    st.title("Scholarly Similarity Batch Viewer")
    st.caption("Local-first batch runner and viewer for artifacts under runs/batches/.")

    _render_sidebar()

    load_error = st.session_state.get("ui_batch_load_error")
    if load_error:
        st.error(load_error)

    run_tab, baselines_tab, presets_tab, history_tab, experiments_tab, cohort_studies_tab, inbox_tab, export_tab, benchmark_health_tab, benchmark_curation_tab, overview_tab, explorer_tab, detail_tab, failures_tab, diagnostics_tab, comparison_tab, reply_tab = st.tabs(
        [
            "Run Batch",
            "Accepted Baselines",
            "Presets",
            "History",
            "Experiments",
            "Cohort Studies",
            "Workspace Inbox",
            "Export Bundle",
            "Benchmark Health",
            "Benchmark Curation",
            "Overview",
            "Seed Explorer",
            "Seed Detail",
            "Worst Cases / Failures",
            "Diagnostics",
            "Comparison",
            "Candidate Reply",
        ]
    )
    with run_tab:
        _render_run_batch_tab()
    with baselines_tab:
        _render_accepted_baselines_tab()
    with presets_tab:
        _render_presets_tab()
    with history_tab:
        _render_history_tab()
    with experiments_tab:
        _render_experiments_tab()
    with cohort_studies_tab:
        _render_cohort_study_registry_tab()
    with inbox_tab:
        _render_workspace_inbox_tab()
    with export_tab:
        _render_export_bundle_tab()
    with benchmark_health_tab:
        _render_benchmark_health_tab()
    with benchmark_curation_tab:
        _render_benchmark_curation_tab()
    with overview_tab:
        _render_overview(st.session_state.get("ui_loaded_batch"))
    with explorer_tab:
        _render_seed_explorer(st.session_state.get("ui_loaded_batch"))
    with detail_tab:
        _render_seed_detail(st.session_state.get("ui_loaded_batch"))
    with failures_tab:
        _render_worst_cases_and_failures(st.session_state.get("ui_loaded_batch"))
    with diagnostics_tab:
        _render_diagnostics(st.session_state.get("ui_loaded_batch"))
    with comparison_tab:
        _render_comparison(st.session_state.get("ui_loaded_batch"))
    with reply_tab:
        _render_candidate_reply_tab()


def _initialize_session_state() -> None:
    for key, default_value in SESSION_DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = default_value


def _sync_batch_dir_widget_from_state_before_render(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    if bool(state.get(BATCH_DIR_WIDGET_SYNC_PENDING_KEY)):
        state[BATCH_DIR_WIDGET_KEY] = str(state.get(BATCH_DIR_STATE_KEY, ""))
        state[BATCH_DIR_WIDGET_SYNC_PENDING_KEY] = False
        return

    if BATCH_DIR_WIDGET_KEY not in state:
        state[BATCH_DIR_WIDGET_KEY] = str(state.get(BATCH_DIR_STATE_KEY, ""))


def _get_selected_batch_dir_from_widget(
    session_state: MutableMapping[str, Any] | None = None,
) -> str:
    state = st.session_state if session_state is None else session_state
    return (
        str(state.get(BATCH_DIR_WIDGET_KEY, "")).strip()
        or str(state.get("ui_batch_dir_choice", "")).strip()
    )


def _sync_theory_config_widget_from_state_before_render(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    if bool(state.get(THEORY_CONFIG_WIDGET_SYNC_PENDING_KEY)):
        state[THEORY_CONFIG_WIDGET_KEY] = str(state.get(THEORY_CONFIG_STATE_KEY, ""))
        state[THEORY_CONFIG_WIDGET_SYNC_PENDING_KEY] = False
        return

    if THEORY_CONFIG_WIDGET_KEY not in state:
        state[THEORY_CONFIG_WIDGET_KEY] = str(state.get(THEORY_CONFIG_STATE_KEY, ""))


def _get_selected_theory_config_from_widget(
    session_state: MutableMapping[str, Any] | None = None,
) -> str:
    state = st.session_state if session_state is None else session_state
    return str(state.get(THEORY_CONFIG_WIDGET_KEY, "")).strip() or str(state.get(THEORY_CONFIG_STATE_KEY, "")).strip()


def _apply_pending_run_batch_prefills_before_render(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    payload = state.get("ui_run_prefill_payload")
    if not isinstance(payload, dict) or not payload:
        return

    if "theory_config_path" in payload:
        state[THEORY_CONFIG_STATE_KEY] = str(payload["theory_config_path"] or "")
        state[THEORY_CONFIG_WIDGET_SYNC_PENDING_KEY] = True
    if "seeds_csv_path" in payload:
        state["ui_seeds_csv_path"] = str(payload["seeds_csv_path"] or "")
    if "max_references" in payload and payload["max_references"] is not None:
        state["ui_run_max_references"] = int(payload["max_references"])
    if "max_related" in payload and payload["max_related"] is not None:
        state["ui_run_max_related"] = int(payload["max_related"])
    if "max_hard_negatives" in payload and payload["max_hard_negatives"] is not None:
        state["ui_run_max_hard_negatives"] = int(payload["max_hard_negatives"])
    if "top_k" in payload and payload["top_k"] is not None:
        state["ui_run_top_k"] = int(payload["top_k"])
    if "label_source" in payload and payload["label_source"] is not None:
        state["ui_run_label_source"] = str(payload["label_source"])
    if "refresh" in payload and payload["refresh"] is not None:
        state["ui_run_refresh"] = bool(payload["refresh"])

    state["ui_run_prefill_payload"] = None


def _render_sidebar() -> None:
    st.sidebar.header("Batch Loader")
    _sync_batch_dir_widget_from_state_before_render()
    available_batch_dirs = discover_batch_dirs()
    available_options = [""] + [_display_path(path) for path in available_batch_dirs]
    st.sidebar.selectbox(
        "Detected batches",
        options=available_options,
        help="Optional shortcut to a locally detected runs/batches directory.",
        key="ui_batch_dir_choice",
    )
    st.sidebar.text_input(
        "Batch directory",
        placeholder="runs/batches/batch_002",
        key=BATCH_DIR_WIDGET_KEY,
    )

    if st.sidebar.button("Load Batch", type="primary", use_container_width=True):
        _load_selected_batch()

    loaded_bundle = st.session_state.get("ui_loaded_batch")
    if loaded_bundle is not None:
        st.sidebar.success(f"Loaded: {loaded_bundle.manifest.batch_id}")
        st.sidebar.caption(_display_path(loaded_bundle.batch_dir))


def _load_selected_batch() -> None:
    candidate_path = _get_selected_batch_dir_from_widget()
    if not candidate_path:
        st.session_state["ui_batch_load_error"] = "Enter a batch directory path or choose one of the detected batches."
        return

    try:
        bundle = load_batch_bundle(candidate_path)
    except BatchLoadError as exc:
        st.session_state["ui_batch_load_error"] = str(exc)
        return

    _apply_loaded_batch(bundle)
    st.session_state["ui_batch_load_error"] = None


def _load_secondary_comparison_batch() -> None:
    candidate_path = str(st.session_state.get("ui_comparison_secondary_dir_input", "")).strip()
    if not candidate_path:
        st.session_state["ui_comparison_error"] = "Enter a secondary batch directory path to compare."
        return

    try:
        bundle = load_batch_bundle(candidate_path)
    except BatchLoadError as exc:
        st.session_state["ui_comparison_error"] = str(exc)
        return

    _apply_secondary_comparison_bundle(bundle)


def _apply_secondary_comparison_bundle(bundle: BatchUiBundle) -> None:
    st.session_state["ui_comparison_secondary_bundle"] = bundle
    st.session_state["ui_comparison_error"] = None
    _clear_comparison_save_feedback()
    _clear_review_packet_feedback()
    _clear_reeval_outcome_feedback()
    _clear_baseline_promotion_feedback()


def _load_review_packet_from_ui() -> None:
    candidate_path = str(st.session_state.get("ui_reply_packet_dir_input", "")).strip()
    if not candidate_path:
        st.session_state["ui_reply_packet_error"] = "Enter a review packet directory path."
        return

    try:
        bundle = load_review_packet_bundle(candidate_path)
    except ReviewPacketLoadError as exc:
        st.session_state["ui_reply_packet_error"] = str(exc)
        return

    st.session_state["ui_loaded_reply_packet"] = bundle
    st.session_state["ui_reply_packet_error"] = None
    _clear_reply_preview_feedback()
    _clear_candidate_apply_feedback()


def _preview_candidate_reply_from_ui() -> None:
    packet_bundle = st.session_state.get("ui_loaded_reply_packet")
    if not isinstance(packet_bundle, ReviewPacketBundle):
        st.session_state["ui_reply_preview_error"] = "Load a review packet before previewing a candidate reply."
        st.session_state["ui_reply_preview_result"] = None
        _clear_candidate_apply_feedback()
        return

    reply_path = str(st.session_state.get("ui_candidate_reply_path", "")).strip()
    if not reply_path:
        st.session_state["ui_reply_preview_error"] = "Enter a candidate reply YAML path."
        st.session_state["ui_reply_preview_result"] = None
        _clear_candidate_apply_feedback()
        return

    try:
        result = preview_candidate_reply(
            packet_bundle=packet_bundle,
            reply_path=reply_path,
        )
    except ReviewPacketLoadError as exc:
        st.session_state["ui_reply_preview_error"] = str(exc)
        st.session_state["ui_reply_preview_result"] = None
        _clear_candidate_apply_feedback()
        return

    st.session_state["ui_reply_preview_error"] = None
    st.session_state["ui_reply_preview_result"] = result
    _clear_candidate_apply_feedback()


def _render_run_batch_tab() -> None:
    st.subheader("Run Batch")
    st.caption("The Initial DOI is UI context only in v1. The backend batch run remains seeds.csv-driven.")
    _apply_pending_run_batch_prefills_before_render()
    _sync_theory_config_widget_from_state_before_render()

    selected_launch_profile_id = _optional_str(st.session_state.get("ui_selected_launch_profile_id"))
    selected_launch_profile_path = _optional_str(st.session_state.get("ui_selected_launch_profile_path"))
    selected_launch_profile_values = st.session_state.get("ui_selected_launch_profile_values")
    selected_launch_profile_warning = _optional_str(st.session_state.get("ui_selected_launch_profile_warning"))
    selected_baseline_id = _optional_str(st.session_state.get("ui_selected_accepted_baseline_id"))
    selected_baseline_theory_path = _optional_str(st.session_state.get("ui_selected_accepted_baseline_theory_path"))
    selected_benchmark_preset_id = _optional_str(st.session_state.get("ui_selected_benchmark_preset_id"))
    selected_benchmark_seeds_csv = _optional_str(st.session_state.get("ui_selected_benchmark_preset_seeds_csv"))
    selected_benchmark_warning = _optional_str(st.session_state.get("ui_selected_benchmark_preset_warning"))
    selected_eval_preset_id = _optional_str(st.session_state.get("ui_selected_eval_preset_id"))
    selected_eval_values = st.session_state.get("ui_selected_eval_preset_values")
    selected_eval_warning = _optional_str(st.session_state.get("ui_selected_eval_preset_warning"))
    current_theory_config = _get_selected_theory_config_from_widget()
    current_seeds_csv = _optional_str(st.session_state.get("ui_seeds_csv_path"))
    if selected_launch_profile_id is not None:
        st.info(f"Selected launch profile for convenience: `{selected_launch_profile_id}`")
        if selected_launch_profile_path is not None:
            st.caption(f"Launch profile JSON: `{selected_launch_profile_path}`")
        if isinstance(selected_launch_profile_values, dict):
            if not _run_batch_form_matches_selected_launch_profile(st.session_state, selected_launch_profile_values):
                st.caption(
                    "One or more launch-profile values have been manually overridden; "
                    "the selected launch profile remains stored in this UI session only."
                )
        if selected_launch_profile_warning:
            st.warning(selected_launch_profile_warning)
    if selected_baseline_id is not None:
        st.info(f"Selected accepted baseline for convenience: `{selected_baseline_id}`")
        if selected_baseline_theory_path is not None:
            st.caption(f"Accepted theory snapshot: `{selected_baseline_theory_path}`")
            if not _path_exists(selected_baseline_theory_path):
                st.warning("The selected accepted baseline snapshot no longer exists on disk.")
            elif _normalized_path_string(current_theory_config) != _normalized_path_string(selected_baseline_theory_path):
                st.caption("Theory config has been manually overridden; the selected baseline remains stored in this UI session only.")
    if selected_benchmark_preset_id is not None:
        st.info(f"Selected benchmark preset for convenience: `{selected_benchmark_preset_id}`")
        if selected_benchmark_seeds_csv is not None:
            st.caption(f"Benchmark seeds CSV: `{selected_benchmark_seeds_csv}`")
            if not _path_exists(selected_benchmark_seeds_csv):
                st.warning("The selected benchmark preset seeds CSV no longer exists on disk.")
            elif _normalized_path_string(current_seeds_csv) != _normalized_path_string(selected_benchmark_seeds_csv):
                st.caption("Seeds CSV has been manually overridden; the selected benchmark preset remains stored in this UI session only.")
        if selected_benchmark_warning:
            st.warning(selected_benchmark_warning)
    if selected_eval_preset_id is not None:
        st.info(f"Selected evaluation preset for convenience: `{selected_eval_preset_id}`")
        if isinstance(selected_eval_values, dict):
            st.caption(
                "Evaluation options: "
                f"refs={selected_eval_values.get('max_references', 'n/a')}, "
                f"related={selected_eval_values.get('max_related', 'n/a')}, "
                f"hard_negatives={selected_eval_values.get('max_hard_negatives', 'n/a')}, "
                f"top_k={selected_eval_values.get('top_k', 'n/a')}, "
                f"label_source={selected_eval_values.get('label_source', 'n/a')}, "
                f"refresh={selected_eval_values.get('refresh', 'n/a')}"
            )
            if not _run_batch_eval_values_match_selected_preset(st.session_state, selected_eval_values):
                st.caption("One or more evaluation options have been manually overridden; the selected evaluation preset remains stored in this UI session only.")
        if selected_eval_warning:
            st.warning(selected_eval_warning)

    with st.form("ui_run_batch_form", clear_on_submit=False):
        st.text_input(
            "Initial DOI (optional UI context only in v1)",
            key="ui_initial_doi_context",
            help="This is saved in UI state for context only and does not change the backend batch semantics in v1.",
        )
        st.text_input("Theory config path", key=THEORY_CONFIG_WIDGET_KEY)
        st.text_input("Seeds CSV path", key="ui_seeds_csv_path")
        st.text_input("Batch ID", key="ui_run_batch_id")
        with st.expander("Advanced Options"):
            st.number_input(
                "max_references",
                min_value=0,
                step=1,
                key="ui_run_max_references",
            )
            st.number_input(
                "max_related",
                min_value=0,
                step=1,
                key="ui_run_max_related",
            )
            st.number_input(
                "max_hard_negatives",
                min_value=0,
                step=1,
                key="ui_run_max_hard_negatives",
            )
            st.number_input(
                "top_k",
                min_value=1,
                step=1,
                key="ui_run_top_k",
            )
            st.selectbox(
                "label_source",
                options=list(SUPPORTED_LABEL_SOURCES),
                key="ui_run_label_source",
                help="Current backend batch execution supports silver labels only.",
            )
            st.checkbox("refresh", key="ui_run_refresh")
        submitted = st.form_submit_button(
            "Run Batch",
            type="primary",
            disabled=bool(st.session_state.get("ui_run_in_progress")),
        )

    if submitted:
        _submit_run_batch_form()

    _render_last_run_summary()


def _render_accepted_baselines_tab() -> None:
    st.subheader("Accepted Baselines")
    st.caption(
        "Browse accepted baseline artifacts under `runs/accepted_baselines/` and explicitly "
        "choose one as a convenience prefill for the Run Batch theory-config field."
    )

    entries, warnings = scan_accepted_baselines()
    if warnings:
        with st.expander(f"Registry warnings ({len(warnings)})"):
            for warning_message in warnings:
                st.warning(warning_message)

    if not entries:
        st.info("No accepted baselines were found under runs/accepted_baselines/.")
        return

    registry_rows = build_accepted_baseline_registry_rows(entries)
    st.dataframe(
        _rows_to_table_data(registry_rows),
        use_container_width=True,
        hide_index=True,
    )

    preferred_baseline_id = (
        _optional_str(st.session_state.get("ui_registry_selected_baseline_id"))
        or _optional_str(st.session_state.get("ui_selected_accepted_baseline_id"))
    )
    default_baseline_id = choose_default_accepted_baseline_id(
        entries,
        preferred_baseline_id=preferred_baseline_id,
    )
    if default_baseline_id is None:
        st.info("No selectable accepted baseline entries are available.")
        return
    if st.session_state.get("ui_registry_selected_baseline_id") not in {
        entry.baseline_id for entry in entries
    }:
        st.session_state["ui_registry_selected_baseline_id"] = default_baseline_id

    st.selectbox(
        "Accepted baseline",
        options=[entry.baseline_id for entry in entries],
        key="ui_registry_selected_baseline_id",
    )
    selected_entry = find_accepted_baseline_entry(
        entries,
        _optional_str(st.session_state.get("ui_registry_selected_baseline_id")),
    )
    if selected_entry is None:
        st.info("Select an accepted baseline to inspect its provenance and theory snapshot.")
        return

    currently_selected_baseline_id = _optional_str(st.session_state.get("ui_selected_accepted_baseline_id"))
    if currently_selected_baseline_id is not None and not any(
        entry.baseline_id == currently_selected_baseline_id for entry in entries
    ):
        st.warning(
            "The currently selected accepted baseline is no longer present in the registry. "
            "You can choose another one below or clear the session-state selection."
        )

    detail = build_accepted_baseline_detail(selected_entry)
    identity = detail["identity"]
    source_lineage = detail["source_lineage"]
    source_primary_batch = detail["source_primary_batch"]
    source_secondary_batch = detail["source_secondary_batch"]
    outcome_summary = detail["outcome_summary"]

    if currently_selected_baseline_id == selected_entry.baseline_id:
        st.success("This accepted baseline is currently selected for Run Batch convenience prefill.")

    detail_columns = st.columns(4)
    detail_columns[0].metric("Baseline ID", identity.get("baseline_id") or "n/a")
    detail_columns[1].metric("Decision Status", identity.get("decision_status") or "n/a")
    detail_columns[2].metric("Selected Metric", _metric_label(identity.get("selected_metric")))
    detail_columns[3].metric("Reviewer", identity.get("reviewer") or "n/a")

    action_columns = st.columns([1.5, 1.1])
    theory_snapshot_path = identity.get("accepted_theory_snapshot_path")
    theory_snapshot_available = theory_snapshot_path is not None and Path(theory_snapshot_path).exists()
    with action_columns[0]:
        if st.button(
            "Use this baseline for Run Batch",
            type="primary",
            use_container_width=True,
            disabled=not theory_snapshot_available,
        ):
            _select_accepted_baseline_for_run_batch(selected_entry)
            st.rerun()
    with action_columns[1]:
        if st.button(
            "Clear selected baseline",
            use_container_width=True,
            disabled=not bool(currently_selected_baseline_id),
        ):
            _clear_selected_accepted_baseline()
            st.rerun()
    if not theory_snapshot_available:
        st.warning("This accepted baseline does not expose a usable accepted_theory_snapshot.yaml path.")

    left_column, right_column = st.columns([1.15, 1.0])
    with left_column:
        st.markdown(f"**Baseline Directory:** `{_display_path(identity.get('baseline_dir')) or 'n/a'}`")
        st.markdown(
            f"**Accepted Theory Snapshot:** `{_display_path(identity.get('accepted_theory_snapshot_path')) or 'n/a'}`"
        )
        st.markdown(
            f"**Candidate Reply:** `{_display_path(identity.get('candidate_reply_yaml_path')) or 'n/a'}`"
        )
        st.markdown(
            f"**Applied Changes:** `{_display_path(identity.get('applied_changes_path')) or 'n/a'}`"
        )
        st.markdown(f"**Created At:** `{identity.get('created_at') or 'n/a'}`")
        st.markdown(f"**Notes:** `{identity.get('notes') or 'n/a'}`")
    with right_column:
        st.markdown("**Source Lineage**")
        st.code(
            _seed_detail_text_block(
                {
                    "comparison_id": source_lineage.get("comparison_id") or "n/a",
                    "packet_id": source_lineage.get("packet_id") or "n/a",
                    "candidate_id": source_lineage.get("candidate_id") or "n/a",
                    "outcome_id": source_lineage.get("outcome_id") or "n/a",
                    "candidate_run_dir": _display_path(source_lineage.get("candidate_run_dir")) or "n/a",
                    "outcome_dir": _display_path(source_lineage.get("outcome_dir")) or "n/a",
                }
            ),
            language="text",
        )

    batch_columns = st.columns(2)
    with batch_columns[0]:
        st.markdown("**Source Primary Batch**")
        st.code(
            _seed_detail_text_block(
                {
                    "batch_id": source_primary_batch.get("batch_id") or "n/a",
                    "batch_dir": _display_path(source_primary_batch.get("batch_dir")) or "n/a",
                    "theory_config": _display_path(source_primary_batch.get("theory_config")) or "n/a",
                }
            ),
            language="text",
        )
    with batch_columns[1]:
        st.markdown("**Source Secondary Batch**")
        st.code(
            _seed_detail_text_block(
                {
                    "batch_id": source_secondary_batch.get("batch_id") or "n/a",
                    "batch_dir": _display_path(source_secondary_batch.get("batch_dir")) or "n/a",
                    "theory_config": _display_path(source_secondary_batch.get("theory_config")) or "n/a",
                }
            ),
            language="text",
        )

    st.markdown("**Outcome Summary**")
    st.code(
        _seed_detail_text_block(
            {
                "common_doi_count": _display_summary_value(outcome_summary.get("common_doi_count")),
                "common_completed_seed_count": _display_summary_value(outcome_summary.get("common_completed_seed_count")),
                "primary_mean": _display_summary_value(outcome_summary.get("primary_mean")),
                "primary_median": _display_summary_value(outcome_summary.get("primary_median")),
                "secondary_mean": _display_summary_value(outcome_summary.get("secondary_mean")),
                "secondary_median": _display_summary_value(outcome_summary.get("secondary_median")),
                "raw_delta_mean": _display_summary_value(outcome_summary.get("raw_delta_mean")),
                "raw_delta_median": _display_summary_value(outcome_summary.get("raw_delta_median")),
                "improvement_delta_mean": _display_summary_value(outcome_summary.get("improvement_delta_mean")),
                "improvement_delta_median": _display_summary_value(outcome_summary.get("improvement_delta_median")),
                "wins": _display_summary_value(outcome_summary.get("wins")),
                "losses": _display_summary_value(outcome_summary.get("losses")),
                "ties": _display_summary_value(outcome_summary.get("ties")),
            }
        ),
        language="text",
    )

    with st.expander("Raw accepted_baseline_manifest.json"):
        st.json(detail["raw_manifest"])
    if detail["promotion_record"] is not None:
        with st.expander("promotion_record.json"):
            st.json(detail["promotion_record"])


def _render_presets_tab() -> None:
    st.subheader("Presets")
    st.caption(
        "Save reusable benchmark, evaluation, and launch presets, then explicitly choose "
        "them as Run Batch prefills without locking the form."
    )

    benchmark_tab, eval_tab, launch_profiles_tab = st.tabs(
        ["Benchmark Presets", "Evaluation Presets", "Launch Profiles"]
    )
    with benchmark_tab:
        _render_benchmark_presets_section()
    with eval_tab:
        _render_evaluation_presets_section()
    with launch_profiles_tab:
        _render_launch_profiles_section()


def _render_history_tab() -> None:
    st.subheader("History")
    st.caption(
        "Browse local provenance across batches, comparisons, review packets, candidate runs, "
        "re-eval outcomes, and accepted baselines. This view is read-only apart from explicit "
        "chooser conveniences."
    )

    entries, warnings = scan_provenance_timeline()
    if warnings:
        with st.expander(f"History scan warnings ({len(warnings)})"):
            for warning_message in warnings:
                st.warning(warning_message)

    if not entries:
        st.info("No history artifacts were found under runs/.")
        return

    available_types = sorted({entry.artifact_type for entry in entries})
    current_types = [
        artifact_type
        for artifact_type in st.session_state.get("ui_history_artifact_types", [])
        if artifact_type in available_types
    ]
    if current_types != st.session_state.get("ui_history_artifact_types", []):
        st.session_state["ui_history_artifact_types"] = current_types

    available_statuses = available_timeline_statuses(entries)
    current_statuses = [
        status
        for status in st.session_state.get("ui_history_statuses", [])
        if status in available_statuses
    ]
    if current_statuses != st.session_state.get("ui_history_statuses", []):
        st.session_state["ui_history_statuses"] = current_statuses

    filter_columns = st.columns([1.1, 1.1, 1.6])
    with filter_columns[0]:
        st.multiselect(
            "Artifact types",
            options=available_types,
            key="ui_history_artifact_types",
        )
    with filter_columns[1]:
        st.multiselect(
            "Statuses / decisions",
            options=available_statuses,
            key="ui_history_statuses",
        )
    with filter_columns[2]:
        st.text_input(
            "Search ids / paths / summary",
            key="ui_history_search_text",
            placeholder="comparison_001, batch_005, candidate_001...",
        )

    filtered_entries = filter_timeline_entries(
        entries,
        artifact_types=list(st.session_state.get("ui_history_artifact_types", [])),
        statuses=list(st.session_state.get("ui_history_statuses", [])),
        search_text=_optional_str(st.session_state.get("ui_history_search_text")),
    )
    if not filtered_entries:
        st.info("No history entries match the current filters.")
        return

    st.caption(f"Showing {len(filtered_entries)} of {len(entries)} history entries, newest first.")
    st.dataframe(
        _rows_to_table_data(build_timeline_rows(filtered_entries)),
        use_container_width=True,
        hide_index=True,
    )

    default_entry_key = choose_default_timeline_entry_key(
        filtered_entries,
        preferred_entry_key=_optional_str(st.session_state.get("ui_history_selected_entry_key")),
    )
    if (
        default_entry_key is not None
        and st.session_state.get("ui_history_selected_entry_key")
        not in {entry.entry_key for entry in filtered_entries}
    ):
        st.session_state["ui_history_selected_entry_key"] = default_entry_key

    entry_labels = {
        entry.entry_key: _timeline_entry_label(entry)
        for entry in filtered_entries
    }
    st.selectbox(
        "History entry",
        options=list(entry_labels.keys()),
        format_func=entry_labels.get,
        key="ui_history_selected_entry_key",
    )

    selected_entry = find_timeline_entry(
        filtered_entries,
        _optional_str(st.session_state.get("ui_history_selected_entry_key")),
    )
    if selected_entry is None:
        st.info("Select a history entry to inspect its manifest and provenance details.")
        return

    detail = build_timeline_detail(selected_entry)
    identity = detail["identity"]
    lineage = detail["lineage"]
    key_fields = detail["key_fields"]

    detail_columns = st.columns(4)
    detail_columns[0].metric("Artifact Type", identity.get("artifact_type") or "n/a")
    detail_columns[1].metric("Artifact ID", identity.get("artifact_id") or "n/a")
    detail_columns[2].metric("Status / Decision", identity.get("status") or "n/a")
    detail_columns[3].metric(
        "Timestamp Source",
        "created_at" if identity.get("timestamp_source") == "created_at" else "manifest mtime",
    )

    st.markdown(f"**Summary:** `{identity.get('summary') or 'n/a'}`")
    st.markdown(f"**Artifact Directory:** `{_display_path(identity.get('artifact_dir')) or 'n/a'}`")
    st.markdown(f"**Manifest Path:** `{_display_path(identity.get('manifest_path')) or 'n/a'}`")
    st.markdown(f"**Timestamp:** `{identity.get('timestamp_display') or 'n/a'}`")

    action_columns = st.columns([1.4, 1.4, 2.2])
    if selected_entry.artifact_type == "batch":
        with action_columns[0]:
            if st.button(
                "Load this batch as primary",
                type="primary",
                use_container_width=True,
                key=f"ui_history_load_batch::{selected_entry.entry_key}",
            ):
                try:
                    bundle = load_batch_bundle(selected_entry.artifact_dir)
                except BatchLoadError as exc:
                    st.error(str(exc))
                else:
                    _apply_loaded_batch(bundle)
                    st.session_state["ui_batch_load_error"] = None
                    st.rerun()
    if selected_entry.artifact_type == "accepted_baseline":
        accepted_theory_snapshot_path = key_fields.get("accepted_theory_snapshot_path")
        can_select_baseline = accepted_theory_snapshot_path is not None and Path(
            accepted_theory_snapshot_path
        ).exists()
        with action_columns[1]:
            if st.button(
                "Use this baseline for Run Batch",
                type="primary",
                use_container_width=True,
                key=f"ui_history_use_baseline::{selected_entry.entry_key}",
                disabled=not can_select_baseline,
            ):
                _select_accepted_baseline_for_run_batch(
                    AcceptedBaselineEntry(
                        baseline_id=selected_entry.artifact_id,
                        baseline_dir=selected_entry.artifact_dir,
                        manifest_path=selected_entry.manifest_path,
                        manifest=selected_entry.manifest,
                        promotion_record_path=None,
                        promotion_record=None,
                        accepted_theory_snapshot_path=accepted_theory_snapshot_path,
                    )
                )
                st.rerun()
        if not can_select_baseline:
            st.warning(
                "This accepted baseline does not expose a usable accepted_theory_snapshot.yaml path."
            )

    content_columns = st.columns([1.05, 1.15])
    with content_columns[0]:
        st.markdown("**Lineage**")
        display_lineage: dict[str, Any] = {}
        for key, value in lineage.items():
            if key.endswith("_dir") or key.endswith("_path"):
                display_lineage[key] = _display_path(value) or "n/a"
            else:
                display_lineage[key] = value if value not in (None, "") else "n/a"
        st.code(
            _seed_detail_text_block(display_lineage),
            language="text",
        )
    with content_columns[1]:
        st.markdown("**Key Manifest Fields**")
        display_key_fields: dict[str, Any] = {}
        for key, value in key_fields.items():
            if isinstance(value, dict):
                display_key_fields[key] = value if value else "n/a"
            elif key.endswith("_dir") or key.endswith("_path") or key in {
                "seeds_csv",
                "theory_config",
            }:
                display_key_fields[key] = _display_path(value) or "n/a"
            else:
                display_key_fields[key] = value if value not in (None, "") else "n/a"
        st.code(_seed_detail_text_block(display_key_fields), language="text")

    run_context_warning = _optional_str(detail.get("run_context_warning"))
    if run_context_warning:
        st.warning(run_context_warning)

    run_context_summary = detail.get("run_context_summary")
    if selected_entry.artifact_type == "batch" and isinstance(run_context_summary, dict) and run_context_summary:
        st.markdown("**Run Context Provenance**")
        display_run_context = {
            key: (_display_path(value) or "n/a") if key.endswith("_dir") or key.endswith("_path") else value
            for key, value in run_context_summary.items()
        }
        st.code(_seed_detail_text_block(display_run_context), language="text")

    nested_fields = {
        key: value
        for key, value in key_fields.items()
        if isinstance(value, dict) and value
    }
    if nested_fields:
        with st.expander("Nested manifest fields"):
            st.json(nested_fields)

    with st.expander("Raw manifest JSON"):
        st.json(detail["raw_manifest"])
    if selected_entry.artifact_type == "batch" and isinstance(detail.get("run_context_payload"), dict):
        with st.expander("Raw run_context.json"):
            st.json(detail["run_context_payload"])


def _render_experiments_tab() -> None:
    st.subheader("Experiments")
    st.caption(
        "Compare local batch runs side by side only when they share the same benchmark/evaluation cohort. "
        "This view is read-only apart from explicit batch-load conveniences."
    )

    rows, warnings = scan_experiment_batches()
    if warnings:
        with st.expander(f"Experiment scan warnings ({len(warnings)})"):
            for warning_message in warnings:
                st.warning(warning_message)

    if not rows:
        st.info("No batch runs were found under `runs/batches/`.")
        return

    current_primary_bundle = st.session_state.get("ui_loaded_batch")
    current_primary_row = (
        find_experiment_row(rows, getattr(getattr(current_primary_bundle, "manifest", None), "batch_id", None))
        if isinstance(current_primary_bundle, BatchUiBundle)
        else None
    )
    current_primary_cohort_key = (
        current_primary_row.cohort_key if isinstance(current_primary_row, ExperimentBatchRow) else None
    )

    context_summary = {
        "current_primary_batch": current_primary_row.batch_id if current_primary_row is not None else "n/a",
        "current_primary_cohort": current_primary_row.cohort_summary if current_primary_row is not None else "n/a",
    }
    st.code(_seed_detail_text_block(context_summary), language="text")

    cohorts = group_experiment_cohorts(rows)
    if cohorts:
        st.markdown("**Detected Cohorts**")
        st.dataframe(
            _rows_to_table_data(build_cohort_rows(cohorts)),
            use_container_width=True,
            hide_index=True,
        )

    cohort_options = [cohort.cohort_key for cohort in cohorts]
    if not cohort_options:
        st.info("No cohorts could be derived from the scanned batch artifacts.")
        return

    preferred_cohort_key = _optional_str(st.session_state.get("ui_experiments_selected_cohort_key"))
    if preferred_cohort_key not in cohort_options:
        preferred_cohort_key = current_primary_cohort_key if current_primary_cohort_key in cohort_options else cohort_options[0]
        st.session_state["ui_experiments_selected_cohort_key"] = preferred_cohort_key

    if current_primary_cohort_key is None and st.session_state.get("ui_experiments_same_as_primary_only"):
        st.session_state["ui_experiments_same_as_primary_only"] = False

    cohort_by_key = {cohort.cohort_key: cohort for cohort in cohorts}
    filter_columns = st.columns([1.35, 1.0, 1.5, 1.7])
    with filter_columns[0]:
        st.selectbox(
            "Cohort",
            options=cohort_options,
            format_func=lambda key: _experiment_cohort_label(cohort_by_key, key),
            key="ui_experiments_selected_cohort_key",
            disabled=bool(st.session_state.get("ui_experiments_same_as_primary_only")) and current_primary_cohort_key is not None,
        )
    with filter_columns[1]:
        st.checkbox(
            "Same cohort as current primary batch",
            key="ui_experiments_same_as_primary_only",
            disabled=current_primary_cohort_key is None,
        )
    with filter_columns[2]:
        st.multiselect(
            "Statuses",
            options=available_experiment_statuses(rows),
            key="ui_experiments_statuses",
        )
    with filter_columns[3]:
        st.text_input(
            "Search batch / theory / presets",
            key="ui_experiments_search_text",
            placeholder="batch_010, theory_v001, baseline_001...",
        )

    active_cohort_key = (
        current_primary_cohort_key
        if bool(st.session_state.get("ui_experiments_same_as_primary_only")) and current_primary_cohort_key is not None
        else _optional_str(st.session_state.get("ui_experiments_selected_cohort_key"))
    )
    filtered_rows = filter_experiment_rows(
        rows,
        cohort_key=active_cohort_key,
        statuses=list(st.session_state.get("ui_experiments_statuses", [])),
        search_text=_optional_str(st.session_state.get("ui_experiments_search_text")),
    )
    if not filtered_rows:
        st.info("No experiment rows match the current cohort and filter settings.")
        return

    metric_options = [
        metric_name
        for metric_name in METRIC_LABELS
        if any(row.metric_means.get(metric_name) is not None for row in filtered_rows)
    ]
    default_metric = choose_default_leaderboard_metric(
        filtered_rows,
        preferred_metric=_optional_str(st.session_state.get("ui_experiments_metric")),
    )
    if default_metric is None:
        st.info("The selected cohort does not expose any aggregate leaderboard metrics.")
        return
    if st.session_state.get("ui_experiments_metric") not in metric_options:
        st.session_state["ui_experiments_metric"] = default_metric

    metric_columns = st.columns([1.2, 1.5])
    with metric_columns[0]:
        st.selectbox(
            "Leaderboard metric",
            options=metric_options,
            format_func=_metric_label,
            key="ui_experiments_metric",
        )
    selected_metric = _optional_str(st.session_state.get("ui_experiments_metric")) or default_metric
    with metric_columns[1]:
        st.caption(
            "Best-first ordering: lower is better for Brier Score / Expected Calibration Error; "
            "higher is better for Precision@k / Recall@k / nDCG@k."
        )

    sorted_rows = sort_experiment_rows(filtered_rows, leaderboard_metric=selected_metric)
    cohort_summary = cohort_by_key.get(active_cohort_key).summary if active_cohort_key in cohort_by_key else "n/a"
    if active_cohort_key == UNCLASSIFIED_COHORT_KEY:
        st.warning(
            "These runs are unclassified because the benchmark/evaluation metadata is incomplete. "
            "They are shown for inspection, not as a confidently comparable leaderboard."
        )
    else:
        st.caption(f"Selected cohort: `{cohort_summary}`")

    best_row = next((row for row in sorted_rows if row.metric_means.get(selected_metric) is not None), None)
    summary_columns = st.columns(3)
    summary_columns[0].metric("Rows in view", len(sorted_rows))
    summary_columns[1].metric("Leaderboard metric", _metric_label(selected_metric))
    summary_columns[2].metric("Best batch", best_row.batch_id if best_row is not None else "n/a")

    st.dataframe(
        _rows_to_table_data(build_experiment_table_rows(sorted_rows, leaderboard_metric=selected_metric)),
        use_container_width=True,
        hide_index=True,
    )

    available_batch_ids = [row.batch_id for row in sorted_rows]
    selected_batch_id = _optional_str(st.session_state.get("ui_experiments_selected_batch_id"))
    if selected_batch_id not in available_batch_ids:
        st.session_state["ui_experiments_selected_batch_id"] = available_batch_ids[0]

    st.selectbox(
        "Selected experiment batch",
        options=available_batch_ids,
        format_func=lambda batch_id: _experiment_batch_label(find_experiment_row(sorted_rows, batch_id), selected_metric),
        key="ui_experiments_selected_batch_id",
    )
    selected_row = find_experiment_row(sorted_rows, _optional_str(st.session_state.get("ui_experiments_selected_batch_id")))
    if selected_row is None:
        st.info("Select a batch to inspect its experiment details.")
        return

    action_columns = st.columns([1.1, 1.1, 2.0])
    with action_columns[0]:
        if st.button("Load selected batch as primary", type="primary", use_container_width=True):
            try:
                bundle = load_batch_bundle(selected_row.batch_dir)
            except BatchLoadError as exc:
                st.error(str(exc))
            else:
                _apply_loaded_batch(bundle)
                _set_study_source_context_for_comparison(selected_entry, selected_candidate_row)
                st.session_state["ui_batch_load_error"] = None
                st.rerun()
    with action_columns[1]:
        if st.button("Load selected batch as secondary", use_container_width=True):
            try:
                bundle = load_batch_bundle(selected_row.batch_dir)
            except BatchLoadError as exc:
                st.error(str(exc))
            else:
                _apply_secondary_comparison_bundle(bundle)
                _set_study_source_context_for_comparison(selected_entry, selected_candidate_row)
                st.rerun()

    detail = build_experiment_detail(selected_row)
    identity = detail["identity"]
    detail_columns = st.columns(4)
    detail_columns[0].metric("Batch ID", identity.get("batch_id") or "n/a")
    detail_columns[1].metric("Status", identity.get("status") or "n/a")
    detail_columns[2].metric("Comparable", "Yes" if identity.get("comparable") else "No")
    detail_columns[3].metric("Created At Source", identity.get("created_at_source") or "n/a")

    st.markdown(f"**Batch Directory:** `{_display_path(identity.get('batch_dir')) or 'n/a'}`")
    st.markdown(f"**Cohort Key:** `{identity.get('cohort_key') or 'n/a'}`")
    st.markdown(f"**Cohort Summary:** `{identity.get('cohort_summary') or 'n/a'}`")
    if selected_row.cohort_missing_fields:
        st.caption("Missing cohort fields: " + ", ".join(selected_row.cohort_missing_fields))

    content_columns = st.columns([1.05, 1.15])
    with content_columns[0]:
        st.markdown("**Batch Fields**")
        manifest_fields = detail["manifest_fields"]
        st.code(
            _seed_detail_text_block(
                {
                    key: (_display_path(value) or "n/a") if key in {"seeds_csv", "theory_config"} else _display_summary_value(value)
                    for key, value in manifest_fields.items()
                }
            ),
            language="text",
        )
    with content_columns[1]:
        st.markdown("**Aggregate Metrics**")
        aggregate_metrics = detail["aggregate_metrics"]
        if aggregate_metrics:
            metric_lines = {
                metric_name: f"mean={values.get('mean')}, median={values.get('median')}"
                for metric_name, values in aggregate_metrics.items()
            }
            st.code(_seed_detail_text_block(metric_lines), language="text")
        else:
            st.info("No aggregate metrics available for this batch.")

    run_context_summary = detail.get("run_context_summary")
    if isinstance(run_context_summary, dict) and run_context_summary:
        st.markdown("**Run Context Provenance**")
        st.code(
            _seed_detail_text_block(
                {
                    key: (_display_path(value) or "n/a") if key.endswith("_dir") or key.endswith("_path") else value
                    for key, value in run_context_summary.items()
                }
            ),
            language="text",
        )

    with st.expander("Raw batch_manifest.json"):
        st.json(detail["raw_manifest"])
    with st.expander("Raw aggregate_summary.json"):
        st.json(detail["raw_aggregate_summary"])
    if isinstance(detail.get("raw_run_context"), dict):
        with st.expander("Raw run_context.json"):
            st.json(detail["raw_run_context"])

    _render_cohort_analysis_section(
        rows=rows,
        cohorts=cohorts,
        current_primary_row=current_primary_row,
        default_cohort_key=(
            active_cohort_key
            if active_cohort_key not in (None, UNCLASSIFIED_COHORT_KEY)
            else current_primary_cohort_key
        ),
    )


def _render_cohort_analysis_section(
    *,
    rows: list[ExperimentBatchRow],
    cohorts: list[Any],
    current_primary_row: ExperimentBatchRow | None,
    default_cohort_key: str | None,
) -> None:
    st.markdown("---")
    st.subheader("Cohort Analysis")
    st.caption(
        "Choose one comparable cohort and one reference batch, then compare every other batch in that cohort "
        "using the same pairwise alignment and improvement-delta semantics as the Comparison tab. "
        f"Pairwise rows use `{COMPARISON_STATUS_OPTIONS[0]}`."
    )

    comparable_cohorts = [cohort for cohort in cohorts if getattr(cohort, "comparable", False)]
    if not comparable_cohorts:
        st.info("No confidently comparable cohorts are available for reference-relative cohort analysis.")
        return

    current_primary_batch_id = current_primary_row.batch_id if current_primary_row is not None else None
    current_primary_cohort_key = (
        current_primary_row.cohort_key
        if current_primary_row is not None and current_primary_row.comparable
        else None
    )
    cohort_options = [str(cohort.cohort_key) for cohort in comparable_cohorts]
    cohort_by_key = {str(cohort.cohort_key): cohort for cohort in comparable_cohorts}

    preferred_cohort_key = _optional_str(st.session_state.get("ui_cohort_analysis_selected_cohort_key"))
    if preferred_cohort_key not in cohort_options:
        preferred_cohort_key = (
            default_cohort_key
            if default_cohort_key in cohort_options
            else current_primary_cohort_key
            if current_primary_cohort_key in cohort_options
            else cohort_options[0]
        )
        st.session_state["ui_cohort_analysis_selected_cohort_key"] = preferred_cohort_key

    if current_primary_cohort_key is None and st.session_state.get("ui_cohort_analysis_same_as_primary_only"):
        st.session_state["ui_cohort_analysis_same_as_primary_only"] = False

    control_columns = st.columns([1.35, 1.0, 1.1, 1.55])
    with control_columns[0]:
        st.selectbox(
            "Analysis cohort",
            options=cohort_options,
            format_func=lambda key: _experiment_cohort_label(cohort_by_key, key),
            key="ui_cohort_analysis_selected_cohort_key",
            disabled=bool(st.session_state.get("ui_cohort_analysis_same_as_primary_only"))
            and current_primary_cohort_key is not None,
        )
    with control_columns[1]:
        st.checkbox(
            "Use same cohort as current primary batch",
            key="ui_cohort_analysis_same_as_primary_only",
            disabled=current_primary_cohort_key is None,
        )
    with control_columns[2]:
        st.checkbox(
            "Include partial/failed runs",
            key="ui_cohort_analysis_include_partial_failed",
        )
    with control_columns[3]:
        st.text_input(
            "Candidate search",
            key="ui_cohort_analysis_search_text",
            placeholder="batch_010, baseline_001, launch_smoke...",
        )

    active_cohort_key = (
        current_primary_cohort_key
        if bool(st.session_state.get("ui_cohort_analysis_same_as_primary_only"))
        and current_primary_cohort_key is not None
        else _optional_str(st.session_state.get("ui_cohort_analysis_selected_cohort_key"))
    )
    if active_cohort_key not in cohort_by_key:
        st.info("Select a valid comparable cohort to analyze.")
        return

    cohort_rows = filter_experiment_rows(rows, cohort_key=active_cohort_key)
    if not bool(st.session_state.get("ui_cohort_analysis_include_partial_failed")):
        cohort_rows = [
            row
            for row in cohort_rows
            if _optional_str(row.status) == "completed" and (row.failed_seed_count or 0) <= 0
        ]
    if len(cohort_rows) < 2:
        st.info(
            "The selected cohort needs at least two completed, loadable batches before reference-relative analysis "
            "is meaningful."
        )
        return

    bundles_by_id, bundle_warnings = _load_experiment_bundles_for_rows(cohort_rows)
    if bundle_warnings:
        with st.expander(f"Cohort load warnings ({len(bundle_warnings)})"):
            for warning_message in bundle_warnings:
                st.warning(warning_message)

    loadable_rows = [row for row in cohort_rows if row.batch_id in bundles_by_id]
    if len(loadable_rows) < 2:
        st.info("Fewer than two batches in this cohort could be loaded for pairwise analysis.")
        return

    reference_batch_options = [row.batch_id for row in loadable_rows]
    current_primary_in_cohort = (
        current_primary_batch_id if current_primary_batch_id in reference_batch_options else None
    )
    preferred_reference_batch_id = choose_default_reference_batch_id(
        loadable_rows,
        current_primary_batch_id=current_primary_in_cohort,
    )
    if _optional_str(st.session_state.get("ui_cohort_analysis_reference_batch_id")) not in reference_batch_options:
        st.session_state["ui_cohort_analysis_reference_batch_id"] = (
            preferred_reference_batch_id or reference_batch_options[0]
        )

    metric_columns = st.columns([1.35, 1.0, 1.7])
    with metric_columns[0]:
        st.selectbox(
            "Reference batch",
            options=reference_batch_options,
            format_func=lambda batch_id: _cohort_reference_batch_label(
                find_experiment_row(loadable_rows, batch_id)
            ),
            key="ui_cohort_analysis_reference_batch_id",
        )

    reference_row = find_experiment_row(
        loadable_rows,
        _optional_str(st.session_state.get("ui_cohort_analysis_reference_batch_id")),
    )
    if reference_row is None:
        st.info("Select a reference batch to analyze the cohort.")
        return
    reference_bundle = bundles_by_id.get(reference_row.batch_id)
    if reference_bundle is None:
        st.info("The selected reference batch could not be loaded for pairwise analysis.")
        return

    candidate_rows = [row for row in loadable_rows if row.batch_id != reference_row.batch_id]
    if not candidate_rows:
        st.info("The selected cohort contains only the reference batch after filtering.")
        return

    metric_counts = pairwise_metric_availability_counts(
        reference_seed_rows=reference_bundle.seed_table_rows,
        candidate_seed_rows_by_id={
            row.batch_id: bundles_by_id[row.batch_id].seed_table_rows
            for row in candidate_rows
            if row.batch_id in bundles_by_id
        },
    )
    metric_options = [
        metric_name
        for metric_name in METRIC_LABELS
        if metric_counts.get(metric_name, 0) > 0
    ]
    default_metric = choose_default_cohort_analysis_metric(
        metric_counts,
        candidate_count=len(candidate_rows),
        preferred_metric=_optional_str(st.session_state.get("ui_cohort_analysis_metric")),
    )
    if default_metric is None or not metric_options:
        st.info(
            "No shared comparison metrics are available across this cohort relative to the selected reference batch."
        )
        return
    if st.session_state.get("ui_cohort_analysis_metric") not in metric_options:
        st.session_state["ui_cohort_analysis_metric"] = default_metric

    with metric_columns[1]:
        st.selectbox(
            "Pairwise metric",
            options=metric_options,
            format_func=_metric_label,
            key="ui_cohort_analysis_metric",
        )
    selected_metric = _optional_str(st.session_state.get("ui_cohort_analysis_metric")) or default_metric
    with metric_columns[2]:
        availability_summary = ", ".join(
            f"{_metric_label(metric_name)}={metric_counts[metric_name]}"
            for metric_name in metric_options
        )
        st.caption(
            "Metric availability by candidate pair: "
            + (availability_summary if availability_summary else "n/a")
        )

    pairwise_rows = build_pairwise_analysis_rows(
        reference_row=reference_row,
        reference_seed_rows=reference_bundle.seed_table_rows,
        candidate_rows=candidate_rows,
        candidate_seed_rows_by_id={
            row.batch_id: bundles_by_id[row.batch_id].seed_table_rows
            for row in candidate_rows
            if row.batch_id in bundles_by_id
        },
        selected_metric=selected_metric,
    )
    visible_pairwise_rows = sort_pairwise_analysis_rows(
        filter_pairwise_analysis_rows(
            pairwise_rows,
            search_text=_optional_str(st.session_state.get("ui_cohort_analysis_search_text")),
        )
    )
    if not visible_pairwise_rows:
        st.info("No cohort-analysis candidate rows match the current filters.")
        return

    summary_columns = st.columns(4)
    summary_columns[0].metric("Reference batch", reference_row.batch_id)
    summary_columns[1].metric("Candidates in view", len(visible_pairwise_rows))
    summary_columns[2].metric("Pairwise metric", _metric_label(selected_metric))
    best_candidate = next(
        (row.candidate_batch_id for row in visible_pairwise_rows if row.pairwise_status == "usable"),
        "n/a",
    )
    summary_columns[3].metric("Top candidate", best_candidate)

    st.caption(
        "Only batches in the same selected cohort are compared here. "
        "Rows with zero common completed seeds or no paired metric overlap are shown as unusable rather than ranked "
        "as normal winners."
    )
    st.dataframe(
        _rows_to_table_data(build_pairwise_analysis_table_rows(visible_pairwise_rows)),
        use_container_width=True,
        hide_index=True,
    )

    candidate_batch_options = [row.candidate_batch_id for row in visible_pairwise_rows]
    if _optional_str(st.session_state.get("ui_cohort_analysis_selected_candidate_batch_id")) not in candidate_batch_options:
        st.session_state["ui_cohort_analysis_selected_candidate_batch_id"] = candidate_batch_options[0]

    st.selectbox(
        "Selected candidate row",
        options=candidate_batch_options,
        format_func=lambda batch_id: _cohort_candidate_row_label(
            find_pairwise_analysis_row(visible_pairwise_rows, batch_id)
        ),
        key="ui_cohort_analysis_selected_candidate_batch_id",
    )
    selected_pairwise_row = find_pairwise_analysis_row(
        visible_pairwise_rows,
        _optional_str(st.session_state.get("ui_cohort_analysis_selected_candidate_batch_id")),
    )
    if selected_pairwise_row is None:
        st.info("Select a candidate row to inspect pairwise details.")
        return

    action_columns = st.columns([1.1, 1.1, 2.0])
    with action_columns[0]:
        if st.button("Load reference as primary", type="primary", use_container_width=True):
            _apply_loaded_batch(reference_bundle)
            st.session_state["ui_batch_load_error"] = None
            st.rerun()
    with action_columns[1]:
        if st.button("Load selected candidate as secondary", use_container_width=True):
            candidate_bundle = bundles_by_id.get(selected_pairwise_row.candidate_batch_id)
            if candidate_bundle is None:
                st.error("The selected candidate batch could not be loaded as a comparison secondary.")
            else:
                _apply_secondary_comparison_bundle(candidate_bundle)
                st.rerun()

    candidate_row = find_experiment_row(loadable_rows, selected_pairwise_row.candidate_batch_id)
    detail = build_pairwise_analysis_detail(selected_pairwise_row)
    detail_columns = st.columns(4)
    detail_columns[0].metric("Reference", selected_pairwise_row.reference_batch_id)
    detail_columns[1].metric("Candidate", selected_pairwise_row.candidate_batch_id)
    detail_columns[2].metric("Common Completed", selected_pairwise_row.common_completed_seed_count)
    detail_columns[3].metric(
        "Evidence Verdict",
        selected_pairwise_row.guardrail_verdict or "n/a",
    )

    st.markdown(f"**Cohort Summary:** `{selected_pairwise_row.cohort_summary}`")
    st.markdown(f"**Candidate Batch Dir:** `{_display_path(selected_pairwise_row.candidate_batch_dir)}`")

    detail_blocks = st.columns([1.0, 1.0])
    with detail_blocks[0]:
        st.markdown("**Pairwise Summary**")
        st.code(
            _seed_detail_text_block(
                {
                    "selected_metric": _metric_label(selected_pairwise_row.selected_metric),
                    "status_mode": selected_pairwise_row.status_mode,
                    "pairwise_status": selected_pairwise_row.pairwise_status,
                    "common_doi_count": selected_pairwise_row.common_doi_count,
                    "common_completed_seed_count": selected_pairwise_row.common_completed_seed_count,
                    "paired_seed_count": selected_pairwise_row.paired_seed_count,
                    "reference_mean": _display_summary_value(selected_pairwise_row.reference_mean),
                    "reference_median": _display_summary_value(selected_pairwise_row.reference_median),
                    "candidate_mean": _display_summary_value(selected_pairwise_row.candidate_mean),
                    "candidate_median": _display_summary_value(selected_pairwise_row.candidate_median),
                    "improvement_delta_mean": _display_summary_value(
                        selected_pairwise_row.improvement_delta_mean
                    ),
                    "improvement_delta_median": _display_summary_value(
                        selected_pairwise_row.improvement_delta_median
                    ),
                    "wins": selected_pairwise_row.wins,
                    "losses": selected_pairwise_row.losses,
                    "ties": selected_pairwise_row.ties,
                    "tie_rate": _display_summary_value(selected_pairwise_row.tie_rate),
                }
            ),
            language="text",
        )
        if selected_pairwise_row.guardrail_reasons:
            st.markdown("**Guardrail Reasons**")
            for reason in selected_pairwise_row.guardrail_reasons:
                st.write(f"- {reason}")
    with detail_blocks[1]:
        st.markdown("**Candidate Provenance**")
        candidate_provenance = detail.get("candidate_provenance", {})
        st.code(
            _seed_detail_text_block(
                {
                    key: (_display_path(value) or "n/a")
                    if key.endswith("_dir") or key.endswith("_path")
                    else _display_summary_value(value)
                    for key, value in candidate_provenance.items()
                }
            ),
            language="text",
        )

    provenance_columns = st.columns(2)
    with provenance_columns[0]:
        st.markdown("**Reference Run Context**")
        reference_run_context = detail.get("reference_run_context_summary")
        if isinstance(reference_run_context, dict) and reference_run_context:
            st.code(
                _seed_detail_text_block(
                    {
                        key: (_display_path(value) or "n/a")
                        if key.endswith("_dir") or key.endswith("_path")
                        else _display_summary_value(value)
                        for key, value in reference_run_context.items()
                    }
                ),
                language="text",
            )
        else:
            st.info("No run_context provenance is available for the reference batch.")
    with provenance_columns[1]:
        st.markdown("**Candidate Run Context**")
        candidate_run_context = detail.get("candidate_run_context_summary")
        if isinstance(candidate_run_context, dict) and candidate_run_context:
            st.code(
                _seed_detail_text_block(
                    {
                        key: (_display_path(value) or "n/a")
                        if key.endswith("_dir") or key.endswith("_path")
                        else _display_summary_value(value)
                        for key, value in candidate_run_context.items()
                    }
                ),
                language="text",
            )
        else:
            st.info("No run_context provenance is available for the candidate batch.")

    if candidate_row is not None:
        st.caption(
            "Candidate manifest context: "
            f"status=`{candidate_row.status or 'n/a'}`, "
            f"theory=`{_display_path(candidate_row.theory_config) or 'n/a'}`"
        )

    with st.expander("Raw pairwise summary"):
        st.json(detail["summary"])

    _render_cohort_study_section(
        cohort_key=active_cohort_key,
        cohort_summary=reference_row.cohort_summary,
        reference_row=reference_row,
        selected_metric=selected_metric,
        pairwise_rows=sort_pairwise_analysis_rows(pairwise_rows),
    )


def _load_experiment_bundles_for_rows(
    rows: list[ExperimentBatchRow],
) -> tuple[dict[str, BatchUiBundle], list[str]]:
    bundles_by_id: dict[str, BatchUiBundle] = {}
    warnings: list[str] = []
    for row in rows:
        try:
            bundles_by_id[row.batch_id] = load_batch_bundle(row.batch_dir)
        except BatchLoadError as exc:
            warnings.append(f"Skipped `{row.batch_id}` during cohort analysis: {exc}")
    return bundles_by_id, warnings


def _render_cohort_study_section(
    *,
    cohort_key: str | None,
    cohort_summary: str,
    reference_row: ExperimentBatchRow,
    selected_metric: str,
    pairwise_rows: list[CohortPairwiseAnalysisRow],
) -> None:
    st.markdown("---")
    st.subheader("Cohort Study / Candidate Shortlist")
    st.caption(
        "Turn the current reference-relative cohort analysis into a durable, copy-only study artifact. "
        "Suggested shortlist/review/drop decisions are only defaults and can be overridden explicitly."
    )

    if not pairwise_rows:
        st.info("No active cohort-analysis candidate rows are available to study.")
        return

    context_key = build_cohort_study_context_key(
        cohort_key=cohort_key,
        reference_batch_id=reference_row.batch_id,
        selected_metric=selected_metric,
        candidate_batch_ids=[row.candidate_batch_id for row in pairwise_rows],
    )
    context_changed = st.session_state.get("ui_cohort_study_context_key") != context_key
    if context_changed:
        st.session_state["ui_cohort_study_context_key"] = context_key
        st.session_state["ui_cohort_study_decisions"] = normalize_cohort_study_decisions(pairwise_rows, None)
        st.session_state["ui_cohort_study_selected_candidate_batch_id"] = ""

    decisions = normalize_cohort_study_decisions(
        pairwise_rows,
        st.session_state.get("ui_cohort_study_decisions"),
        )
    st.session_state["ui_cohort_study_decisions"] = decisions

    summary = summarize_cohort_study_decisions(
        pairwise_rows,
        decisions,
        reference_batch_id=reference_row.batch_id,
        selected_metric=selected_metric,
    )
    summary_columns = st.columns(6)
    summary_columns[0].metric("Reference", reference_row.batch_id)
    summary_columns[1].metric("Metric", _metric_label(selected_metric))
    summary_columns[2].metric("Candidates", summary["total_candidate_rows"])
    summary_columns[3].metric("Usable", summary["usable_candidate_rows"])
    summary_columns[4].metric("Unusable", summary["unusable_candidate_rows"])
    summary_columns[5].metric("Shortlist", summary["shortlist_count"])
    st.code(
        _seed_detail_text_block(
            {
                "selected_cohort_summary": cohort_summary,
                "reference_batch_id": reference_row.batch_id,
                "selected_metric": _metric_label(selected_metric),
                "candidate_row_count": summary["total_candidate_rows"],
                "usable_candidate_row_count": summary["usable_candidate_rows"],
                "unusable_candidate_row_count": summary["unusable_candidate_rows"],
            }
        ),
        language="text",
    )

    filter_columns = st.columns([1.0, 0.9, 0.9, 1.7])
    with filter_columns[0]:
        st.selectbox(
            "Decision filter",
            options=list(COHORT_STUDY_DECISION_FILTER_OPTIONS),
            key="ui_cohort_study_decision_filter",
        )
    with filter_columns[1]:
        st.checkbox("Only usable", key="ui_cohort_study_only_usable")
    with filter_columns[2]:
        st.checkbox("Only unusable", key="ui_cohort_study_only_unusable")
    with filter_columns[3]:
        st.text_input(
            "Study search",
            key="ui_cohort_study_search_text",
            placeholder="batch_010, baseline_001, launch_profile_001...",
        )

    filtered_rows = filter_cohort_study_rows(
        pairwise_rows,
        decisions,
        decision_filter=str(st.session_state.get("ui_cohort_study_decision_filter", "all")),
        only_usable=bool(st.session_state.get("ui_cohort_study_only_usable")),
        only_unusable=bool(st.session_state.get("ui_cohort_study_only_unusable")),
        search_text=str(st.session_state.get("ui_cohort_study_search_text", "")),
    )
    if not filtered_rows:
        st.info("No cohort-study rows match the current shortlist filters.")
    else:
        st.dataframe(
            _rows_to_table_data(build_cohort_study_table_rows(filtered_rows, decisions)),
            use_container_width=True,
            hide_index=True,
        )

    candidate_options = [row.candidate_batch_id for row in filtered_rows] or [row.candidate_batch_id for row in pairwise_rows]
    if _optional_str(st.session_state.get("ui_cohort_study_selected_candidate_batch_id")) not in candidate_options:
        st.session_state["ui_cohort_study_selected_candidate_batch_id"] = candidate_options[0]

    st.selectbox(
        "Selected study candidate",
        options=candidate_options,
        format_func=lambda batch_id: _cohort_candidate_row_label(
            find_pairwise_analysis_row(pairwise_rows, batch_id)
        ),
        key="ui_cohort_study_selected_candidate_batch_id",
    )
    selected_candidate_row = find_pairwise_analysis_row(
        pairwise_rows,
        _optional_str(st.session_state.get("ui_cohort_study_selected_candidate_batch_id")),
    )
    if selected_candidate_row is None:
        st.info("Select a cohort-study candidate to edit its decision.")
        return

    suggested_decision, suggestion_reasons = suggest_cohort_study_decision(selected_candidate_row)
    current_decision = decisions.get(selected_candidate_row.candidate_batch_id, suggested_decision)
    decision_columns = st.columns([1.0, 1.0, 1.4])
    decision_columns[0].metric("Current decision", current_decision)
    decision_columns[1].metric("Suggested decision", suggested_decision)
    decision_columns[2].metric(
        "Guardrail verdict",
        selected_candidate_row.guardrail_verdict or "n/a",
    )

    decision_widget_key = f"ui_cohort_study_decision_value__{selected_candidate_row.candidate_batch_id}"
    if context_changed or decision_widget_key not in st.session_state:
        st.session_state[decision_widget_key] = current_decision

    update_columns = st.columns([1.0, 1.0, 1.6])
    with update_columns[0]:
        st.selectbox(
            "Decision",
            options=list(COHORT_STUDY_DECISION_OPTIONS),
            key=decision_widget_key,
        )
    with update_columns[1]:
        if st.button("Update Study Decision", use_container_width=True):
            decisions = dict(st.session_state.get("ui_cohort_study_decisions") or {})
            decisions[selected_candidate_row.candidate_batch_id] = str(st.session_state.get(decision_widget_key))
            st.session_state["ui_cohort_study_decisions"] = decisions
            st.rerun()

    if suggestion_reasons:
        st.markdown("**Suggested-decision reasons**")
        for reason in suggestion_reasons:
            st.write(f"- {reason}")

    summary_counts_columns = st.columns(4)
    summary_counts_columns[0].metric("Review", summary["review_count"])
    summary_counts_columns[1].metric("Drop", summary["drop_count"])
    summary_counts_columns[2].metric("Usable", summary["usable_candidate_rows"])
    summary_counts_columns[3].metric("Unusable", summary["unusable_candidate_rows"])

    with st.form("ui_cohort_study_export_form", clear_on_submit=False):
        st.text_input("Study ID", key="ui_cohort_study_id")
        st.text_input("Reviewer", key="ui_cohort_study_reviewer")
        st.text_area("Notes", key="ui_cohort_study_notes", height=120)
        export_options = st.columns(2)
        with export_options[0]:
            st.checkbox(
                "Include Markdown summary",
                key="ui_cohort_study_include_markdown_summary",
            )
        with export_options[1]:
            st.checkbox(
                "Include shortlist CSV",
                key="ui_cohort_study_include_shortlist_csv",
            )
        submitted = st.form_submit_button("Export Cohort Study", type="primary")

    if submitted:
        try:
            request = build_cohort_study_export_request(
                study_id=str(st.session_state.get("ui_cohort_study_id", "")),
                reviewer=str(st.session_state.get("ui_cohort_study_reviewer", "")),
                notes=str(st.session_state.get("ui_cohort_study_notes", "")),
                include_markdown_summary=bool(
                    st.session_state.get("ui_cohort_study_include_markdown_summary")
                ),
                include_shortlist_csv=bool(
                    st.session_state.get("ui_cohort_study_include_shortlist_csv")
                ),
            )
            result = export_cohort_study(
                base_dir=REPO_ROOT / "runs" / "cohort_studies",
                request=request,
                cohort_key=cohort_key,
                cohort_summary=cohort_summary,
                reference_row=reference_row,
                selected_metric=selected_metric,
                pairwise_rows=pairwise_rows,
                decisions=st.session_state.get("ui_cohort_study_decisions"),
            )
        except CohortStudyError as exc:
            st.session_state["ui_cohort_study_error"] = str(exc)
            st.session_state["ui_cohort_study_result"] = None
        else:
            st.session_state["ui_cohort_study_error"] = None
            st.session_state["ui_cohort_study_result"] = result

    study_error = _optional_str(st.session_state.get("ui_cohort_study_error"))
    if study_error is not None:
        st.error(study_error)

    study_result = st.session_state.get("ui_cohort_study_result")
    if isinstance(study_result, CohortStudyExportResult):
        st.success(f"Exported cohort study to `{_display_path(study_result.study_dir)}`")
        st.code(
            _seed_detail_text_block(
                {
                    "study_id": study_result.study_id,
                    "study_dir": _display_path(study_result.study_dir) or "n/a",
                    "cohort_study_manifest": _display_path(study_result.manifest_path) or "n/a",
                    "cohort_leaderboard_jsonl": _display_path(study_result.leaderboard_path) or "n/a",
                    "candidate_decisions_jsonl": _display_path(study_result.decisions_path) or "n/a",
                    "shortlisted_batches_csv": _display_path(study_result.shortlist_csv_path) or "n/a",
                    "cohort_study_report_md": _display_path(study_result.report_path) or "n/a",
                }
            ),
            language="text",
        )


def _render_cohort_study_registry_tab() -> None:
    st.subheader("Cohort Studies")
    st.caption(
        "Browse saved cohort-study bundles under `runs/cohort_studies/`, inspect their candidate queues, "
        "and explicitly load a saved reference/candidate pair back into the existing Comparison workflow."
    )

    entries, warnings = scan_cohort_studies()
    if warnings:
        with st.expander(f"Cohort study scan warnings ({len(warnings)})"):
            for warning_message in warnings:
                st.warning(warning_message)

    if not entries:
        st.info("No saved cohort studies were found under `runs/cohort_studies/`.")
        return

    st.dataframe(
        _rows_to_table_data(build_cohort_study_registry_rows(entries)),
        use_container_width=True,
        hide_index=True,
    )

    default_study_id = choose_default_cohort_study_id(
        entries,
        preferred_study_id=_optional_str(st.session_state.get("ui_study_registry_selected_study_id")),
    )
    available_study_ids = [entry.study_id for entry in entries]
    if _optional_str(st.session_state.get("ui_study_registry_selected_study_id")) not in available_study_ids:
        st.session_state["ui_study_registry_selected_study_id"] = default_study_id or available_study_ids[0]

    st.selectbox(
        "Selected cohort study",
        options=available_study_ids,
        key="ui_study_registry_selected_study_id",
    )
    selected_entry = find_cohort_study_entry(
        entries,
        _optional_str(st.session_state.get("ui_study_registry_selected_study_id")),
    )
    if selected_entry is None:
        st.info("Select a cohort study to inspect its saved review queue.")
        return

    detail = build_cohort_study_detail(selected_entry)
    identity = detail["identity"]
    reference_batch = detail["reference_batch"]
    counts = detail["counts"]

    detail_columns = st.columns(4)
    detail_columns[0].metric("Study ID", identity.get("study_id") or "n/a")
    detail_columns[1].metric("Reference Batch", _optional_str(reference_batch.get("batch_id")) or "n/a")
    detail_columns[2].metric("Selected Metric", _metric_label(_optional_str(identity.get("selected_metric"))))
    detail_columns[3].metric("Shortlist Count", counts.get("shortlist_count") or 0)

    st.markdown(f"**Study Directory:** `{_display_path(identity.get('study_dir')) or 'n/a'}`")
    st.markdown(f"**Cohort Summary:** `{identity.get('cohort_summary') or 'n/a'}`")

    content_columns = st.columns([1.0, 1.05])
    with content_columns[0]:
        st.markdown("**Study Context**")
        st.code(
            _seed_detail_text_block(
                {
                    "created_at": identity.get("created_at") or "n/a",
                    "created_at_source": identity.get("created_at_source") or "n/a",
                    "reviewer": identity.get("reviewer") or "n/a",
                    "notes": identity.get("notes") or "n/a",
                    "total_candidate_rows": counts.get("total_candidate_rows") or 0,
                    "usable_candidate_rows": counts.get("usable_candidate_rows") or 0,
                    "unusable_candidate_rows": counts.get("unusable_candidate_rows") or 0,
                    "shortlist_count": counts.get("shortlist_count") or 0,
                    "review_count": counts.get("review_count") or 0,
                    "drop_count": counts.get("drop_count") or 0,
                }
            ),
            language="text",
        )
    with content_columns[1]:
        st.markdown("**Reference Batch**")
        st.code(
            _seed_detail_text_block(
                {
                    "batch_id": _optional_str(reference_batch.get("batch_id")) or "n/a",
                    "batch_dir": _display_path(reference_batch.get("batch_dir")) or "n/a",
                    "available_on_disk": "yes" if bool(reference_batch.get("batch_exists")) else "no",
                    "status": _optional_str(reference_batch.get("status")) or "n/a",
                    "theory_config": _display_path(reference_batch.get("theory_config")) or "n/a",
                }
            ),
            language="text",
        )

    if not bool(reference_batch.get("batch_exists")):
        st.warning(
            "The saved study reference batch is no longer available on disk, so it cannot be loaded directly."
        )

    if isinstance(detail.get("report_markdown"), str) and detail["report_markdown"].strip():
        with st.expander("Study report markdown"):
            st.markdown(detail["report_markdown"])
    with st.expander("Raw cohort_study_manifest.json"):
        st.json(detail["raw_manifest"])

    candidate_rows = build_cohort_study_candidate_rows(selected_entry)
    if not candidate_rows:
        st.info("This saved study does not expose any candidate queue rows.")
        return

    filter_columns = st.columns([1.0, 0.9, 1.7])
    with filter_columns[0]:
        st.selectbox(
            "Decision filter",
            options=list(COHORT_STUDY_REGISTRY_DECISION_FILTER_OPTIONS),
            key="ui_study_registry_decision_filter",
        )
    with filter_columns[1]:
        st.checkbox("Usable only", key="ui_study_registry_usable_only")
    with filter_columns[2]:
        st.text_input(
            "Queue search",
            key="ui_study_registry_search_text",
            placeholder="batch_010, baseline_001, launch_profile_001...",
        )

    filtered_candidate_rows = filter_cohort_study_candidate_rows(
        candidate_rows,
        decision_filter=str(st.session_state.get("ui_study_registry_decision_filter", "all")),
        usable_only=bool(st.session_state.get("ui_study_registry_usable_only")),
        search_text=_optional_str(st.session_state.get("ui_study_registry_search_text")),
    )
    if not filtered_candidate_rows:
        st.info("No saved-study candidate rows match the current filters.")
        return

    st.dataframe(
        _rows_to_table_data(build_cohort_study_candidate_table_rows(filtered_candidate_rows)),
        use_container_width=True,
        hide_index=True,
    )

    available_candidate_ids = [row.candidate_batch_id for row in filtered_candidate_rows]
    if _optional_str(st.session_state.get("ui_study_registry_selected_candidate_batch_id")) not in available_candidate_ids:
        st.session_state["ui_study_registry_selected_candidate_batch_id"] = available_candidate_ids[0]

    st.selectbox(
        "Selected study candidate",
        options=available_candidate_ids,
        format_func=lambda batch_id: _cohort_study_registry_candidate_label(
            find_cohort_study_candidate_row(filtered_candidate_rows, batch_id)
        ),
        key="ui_study_registry_selected_candidate_batch_id",
    )
    selected_candidate_row = find_cohort_study_candidate_row(
        filtered_candidate_rows,
        _optional_str(st.session_state.get("ui_study_registry_selected_candidate_batch_id")),
    )
    if selected_candidate_row is None:
        st.info("Select a saved-study candidate row to inspect it.")
        return

    action_columns = st.columns([1.1, 1.1, 2.0])
    reference_batch_dir = reference_batch.get("batch_dir")
    can_load_reference = bool(reference_batch.get("batch_exists")) and isinstance(reference_batch_dir, Path)
    can_load_candidate = selected_candidate_row.candidate_batch_exists and isinstance(
        selected_candidate_row.candidate_batch_dir, Path
    )
    with action_columns[0]:
        if st.button(
            "Load study reference as primary",
            type="primary",
            use_container_width=True,
            disabled=not can_load_reference,
            key=f"ui_study_registry_load_reference::{selected_entry.study_id}",
        ):
            try:
                bundle = load_batch_bundle(reference_batch_dir)
            except BatchLoadError as exc:
                st.error(str(exc))
            else:
                _apply_loaded_batch(bundle)
                st.session_state["ui_batch_load_error"] = None
                st.rerun()
    with action_columns[1]:
        if st.button(
            "Load selected candidate as secondary",
            use_container_width=True,
            disabled=not can_load_candidate,
            key=f"ui_study_registry_load_candidate::{selected_entry.study_id}::{selected_candidate_row.candidate_batch_id}",
        ):
            try:
                bundle = load_batch_bundle(selected_candidate_row.candidate_batch_dir)
            except BatchLoadError as exc:
                st.error(str(exc))
            else:
                _apply_secondary_comparison_bundle(bundle)
                st.rerun()

    if not can_load_candidate:
        st.warning(
            "The selected candidate batch is no longer available on disk, so it cannot be loaded into Comparison."
        )

    candidate_detail = build_cohort_study_candidate_detail(selected_candidate_row)
    queue_columns = st.columns(4)
    queue_columns[0].metric("Decision", _optional_str(candidate_detail["identity"].get("decision")) or "n/a")
    queue_columns[1].metric(
        "Suggested",
        _optional_str(candidate_detail["identity"].get("suggested_decision")) or "n/a",
    )
    queue_columns[2].metric(
        "Usable",
        "Yes"
        if candidate_detail["identity"].get("usable") is True
        else "No"
        if candidate_detail["identity"].get("usable") is False
        else "n/a",
    )
    queue_columns[3].metric(
        "Guardrail Verdict",
        _optional_str(candidate_detail["summary"].get("guardrail_verdict")) or "n/a",
    )

    candidate_columns = st.columns([1.0, 1.05])
    with candidate_columns[0]:
        st.markdown("**Candidate Summary**")
        st.code(
            _seed_detail_text_block(
                {
                    "candidate_batch_id": selected_candidate_row.candidate_batch_id,
                    "candidate_batch_dir": _display_path(selected_candidate_row.candidate_batch_dir) or "n/a",
                    "available_on_disk": "yes" if selected_candidate_row.candidate_batch_exists else "no",
                    "selected_metric": _metric_label(_optional_str(candidate_detail["summary"].get("selected_metric"))),
                    "improvement_delta_mean": _display_summary_value(candidate_detail["summary"].get("improvement_delta_mean")),
                    "improvement_delta_median": _display_summary_value(candidate_detail["summary"].get("improvement_delta_median")),
                    "wins": _display_summary_value(candidate_detail["summary"].get("wins")),
                    "losses": _display_summary_value(candidate_detail["summary"].get("losses")),
                    "ties": _display_summary_value(candidate_detail["summary"].get("ties")),
                }
            ),
            language="text",
        )
    with candidate_columns[1]:
        st.markdown("**Candidate Provenance**")
        st.code(
            _seed_detail_text_block(
                {
                    key: (_display_path(value) or "n/a")
                    if key.endswith("_dir") or key.endswith("_path")
                    else _display_summary_value(value)
                    for key, value in candidate_detail["provenance"].items()
                }
            ),
            language="text",
        )

    if candidate_detail["raw_decision_row"] is not None or candidate_detail["raw_leaderboard_row"] is not None:
        with st.expander("Raw candidate row details"):
            st.json(
                {
                    "candidate_decisions_row": candidate_detail["raw_decision_row"],
                    "cohort_leaderboard_row": candidate_detail["raw_leaderboard_row"],
                }
            )


def _render_workspace_inbox_tab() -> None:
    st.subheader("Workspace Inbox")
    st.caption(
        "Surface the most actionable saved artifacts across the local workflow and hand them back "
        "into the existing review steps with explicit button clicks only."
    )
    st.caption(
        "Pending queues are conservative, read-only heuristics based on saved manifests on disk. "
        "Nothing is auto-run, auto-saved, or mutated from this tab."
    )

    inbox_error = _optional_str(st.session_state.get("ui_inbox_error"))
    inbox_feedback = _optional_str(st.session_state.get("ui_inbox_feedback"))
    if inbox_error:
        st.error(inbox_error)
    if inbox_feedback:
        st.success(inbox_feedback)

    items, warnings = scan_workspace_inbox()
    if warnings:
        with st.expander(f"Inbox scan warnings ({len(warnings)})"):
            for warning_message in warnings:
                st.warning(warning_message)

    if not items:
        st.info("No actionable saved artifacts were found yet.")
        return

    baseline_entries, _ = scan_accepted_baselines()
    baseline_by_id = {entry.baseline_id: entry for entry in baseline_entries}

    filter_columns = st.columns([1.5, 1.7, 0.9])
    with filter_columns[0]:
        st.multiselect(
            "Queue types",
            options=list(WORKSPACE_INBOX_QUEUE_TYPES),
            format_func=lambda queue_type: WORKSPACE_INBOX_QUEUE_LABELS.get(queue_type, queue_type),
            key="ui_inbox_queue_types",
        )
    with filter_columns[1]:
        st.text_input(
            "Search ids / lineage / paths",
            key="ui_inbox_search_text",
            placeholder="study_001, packet_003, candidate_002, baseline_002...",
        )
    with filter_columns[2]:
        st.number_input(
            "Newest N / queue",
            min_value=1,
            max_value=50,
            step=1,
            key="ui_inbox_recent_limit",
        )

    filtered_items = filter_workspace_inbox_items(
        items,
        queue_types=list(st.session_state.get("ui_inbox_queue_types", [])),
        search_text=_optional_str(st.session_state.get("ui_inbox_search_text")),
    )
    if not filtered_items:
        st.info("No inbox rows match the current filters.")
        return

    grouped_all = group_workspace_inbox_items(filtered_items)
    grouped_display = group_workspace_inbox_items(
        filtered_items,
        recent_limit=int(st.session_state.get("ui_inbox_recent_limit", 10)),
    )

    selected_queue_types = set(st.session_state.get("ui_inbox_queue_types", [])) or set(WORKSPACE_INBOX_QUEUE_TYPES)
    for queue_type in WORKSPACE_INBOX_QUEUE_TYPES:
        if queue_type not in selected_queue_types:
            continue
        _render_workspace_inbox_queue_section(
            queue_type=queue_type,
            queue_items=grouped_all.get(queue_type, []),
            display_items=grouped_display.get(queue_type, []),
            baseline_by_id=baseline_by_id,
        )


def _render_workspace_inbox_queue_section(
    *,
    queue_type: str,
    queue_items: list[WorkspaceInboxItem],
    display_items: list[WorkspaceInboxItem],
    baseline_by_id: dict[str, AcceptedBaselineEntry],
) -> None:
    st.markdown("---")
    st.markdown(f"### {WORKSPACE_INBOX_QUEUE_LABELS.get(queue_type, queue_type)}")
    st.caption(f"{len(queue_items)} item(s)")

    if not queue_items:
        st.info(_workspace_inbox_empty_message(queue_type))
        return

    if display_items and len(display_items) < len(queue_items):
        st.caption(f"Showing newest {len(display_items)} of {len(queue_items)} item(s).")

    visible_items = display_items or queue_items
    st.dataframe(
        _rows_to_table_data(build_workspace_inbox_table_rows(visible_items)),
        use_container_width=True,
        hide_index=True,
    )

    select_key = f"ui_inbox_selected_item::{queue_type}"
    visible_item_keys = [item.item_key for item in visible_items]
    if _optional_str(st.session_state.get(select_key)) not in visible_item_keys:
        st.session_state[select_key] = choose_default_workspace_inbox_item_key(
            visible_items,
            preferred_item_key=_optional_str(st.session_state.get(select_key)),
        )

    st.selectbox(
        "Selected item",
        options=visible_item_keys,
        format_func=lambda item_key: _workspace_inbox_item_label(find_workspace_inbox_item(visible_items, item_key)),
        key=select_key,
    )
    selected_item = find_workspace_inbox_item(
        visible_items,
        _optional_str(st.session_state.get(select_key)),
    )
    if selected_item is None:
        return

    detail = build_workspace_inbox_detail(selected_item)
    detail_columns = st.columns(4)
    detail_columns[0].metric("Item", detail["identity"].get("item_id") or "n/a")
    detail_columns[1].metric(
        "Status",
        detail["decision"].get("decision_status")
        or detail["decision"].get("decision")
        or detail["decision"].get("status")
        or "n/a",
    )
    detail_columns[2].metric("Metric", _metric_label(detail["decision"].get("selected_metric")))
    detail_columns[3].metric("Study", detail["lineage"].get("study_id") or "n/a")

    st.code(
        _seed_detail_text_block(
            {
                "created_at": detail["identity"].get("created_at") or "n/a",
                "created_at_source": detail["identity"].get("created_at_source") or "n/a",
                "summary": detail["identity"].get("summary") or "n/a",
                "comparison_id": detail["lineage"].get("comparison_id") or "n/a",
                "packet_id": detail["lineage"].get("packet_id") or "n/a",
                "candidate_id": detail["lineage"].get("candidate_id") or "n/a",
                "outcome_id": detail["lineage"].get("outcome_id") or "n/a",
                "baseline_id": detail["lineage"].get("baseline_id") or "n/a",
                "artifact_dir": _display_path(detail["identity"].get("artifact_dir")) or "n/a",
                "primary_batch_id": detail["targets"].get("primary_batch_id") or "n/a",
                "secondary_batch_id": detail["targets"].get("secondary_batch_id") or "n/a",
                "guardrail_verdict": detail["decision"].get("guardrail_verdict") or "n/a",
            }
        ),
        language="text",
    )

    _render_workspace_inbox_actions(item=selected_item, baseline_by_id=baseline_by_id)

    with st.expander("Raw payload"):
        st.json(detail["raw_payload"] if detail["raw_payload"] is not None else {})


def _render_workspace_inbox_actions(
    *,
    item: WorkspaceInboxItem,
    baseline_by_id: dict[str, AcceptedBaselineEntry],
) -> None:
    if item.queue_type == "shortlisted_candidates":
        action_columns = st.columns([1.0, 1.0, 2.0])
        with action_columns[0]:
            if st.button(
                "Load reference as primary",
                type="primary",
                use_container_width=True,
                disabled=not item.target_primary_batch_exists,
                key=f"ui_inbox_load_reference::{item.item_key}",
            ):
                error = _load_inbox_primary_batch(item, preserve_study_source=True)
                if error:
                    _set_inbox_error(error)
                else:
                    _set_inbox_feedback(
                        f"Loaded reference batch '{item.target_primary_batch_id}' from study '{item.study_id}'."
                    )
        with action_columns[1]:
            if st.button(
                "Load candidate as secondary",
                use_container_width=True,
                disabled=not item.target_secondary_batch_exists,
                key=f"ui_inbox_load_candidate::{item.item_key}",
            ):
                error = _load_inbox_secondary_batch(item, preserve_study_source=True)
                if error:
                    _set_inbox_error(error)
                else:
                    _set_inbox_feedback(
                        f"Loaded shortlisted candidate '{item.target_secondary_batch_id}' as the comparison secondary."
                    )
        return

    if item.queue_type == "review_packets_pending_candidate_work":
        if st.button(
            "Load packet into Candidate Reply",
            type="primary",
            use_container_width=True,
            key=f"ui_inbox_load_packet::{item.item_key}",
        ):
            error = _load_review_packet_into_candidate_reply(item.artifact_dir)
            if error:
                _set_inbox_error(error)
            else:
                _set_inbox_feedback(f"Loaded review packet '{item.packet_id}' into Candidate Reply.")
        return

    if item.queue_type == "candidate_runs_pending_outcome":
        if st.button(
            "Load candidate run into Outcome context",
            type="primary",
            use_container_width=True,
            key=f"ui_inbox_load_candidate_run::{item.item_key}",
        ):
            note = _prepare_candidate_run_for_reeval(item)
            if note:
                _set_inbox_feedback(
                    f"Prepared Save Re-eval Outcome for candidate run '{item.candidate_id}'. Note: {note}"
                )
            else:
                _set_inbox_feedback(f"Prepared Save Re-eval Outcome for candidate run '{item.candidate_id}'.")
        return

    if item.queue_type == "accepted_outcomes_pending_promotion":
        if st.button(
            "Load outcome + candidate run into Promotion context",
            type="primary",
            use_container_width=True,
            key=f"ui_inbox_load_promotion::{item.item_key}",
        ):
            note = _prepare_outcome_for_promotion(item)
            if note:
                _set_inbox_feedback(
                    f"Prepared Promote Accepted Baseline for outcome '{item.outcome_id}'. Note: {note}"
                )
            else:
                _set_inbox_feedback(f"Prepared Promote Accepted Baseline for outcome '{item.outcome_id}'.")
        return

    if item.queue_type == "weak_guarded_outcomes":
        if st.button(
            "Load related pair for inspection",
            type="primary",
            use_container_width=True,
            disabled=not (item.target_primary_batch_exists and item.target_secondary_batch_exists),
            key=f"ui_inbox_load_guarded_pair::{item.item_key}",
        ):
            note = _load_inbox_pair_for_inspection(item)
            if note:
                _set_inbox_feedback(
                    f"Loaded weak/guarded outcome '{item.outcome_id}' for inspection. Note: {note}"
                )
            else:
                _set_inbox_feedback(f"Loaded weak/guarded outcome '{item.outcome_id}' pair into Comparison.")
        return

    if item.queue_type == "recent_accepted_baselines":
        entry = baseline_by_id.get(item.baseline_id or "")
        if st.button(
            "Use this baseline for Run Batch",
            type="primary",
            use_container_width=True,
            disabled=entry is None,
            key=f"ui_inbox_use_baseline::{item.item_key}",
        ):
            if entry is None:
                _set_inbox_error("The selected accepted baseline is no longer available on disk.")
            else:
                _select_accepted_baseline_for_run_batch(entry)
                _set_inbox_feedback(f"Selected accepted baseline '{entry.baseline_id}' for Run Batch.")


def _workspace_inbox_item_label(item: WorkspaceInboxItem | None) -> str:
    if item is None:
        return "n/a"
    return f"{item.item_id} | {item.created_at_display} | {item.summary}"


def _workspace_inbox_empty_message(queue_type: str) -> str:
    messages = {
        "shortlisted_candidates": "No shortlisted saved-study candidates were found.",
        "review_packets_pending_candidate_work": "No review packets are currently waiting for candidate reply work.",
        "candidate_runs_pending_outcome": "No candidate runs are currently waiting for a re-eval outcome.",
        "accepted_outcomes_pending_promotion": "No accepted outcomes currently look pending for baseline promotion.",
        "weak_guarded_outcomes": "No weak or failed guardrail outcomes were found.",
        "recent_accepted_baselines": "No accepted baselines were found yet.",
    }
    return messages.get(queue_type, "No items were found.")


def _render_export_bundle_tab() -> None:
    st.subheader("Export Bundle")
    st.caption(
        "Create a copy-only report bundle under `runs/reports/` from the current local UI context. "
        "This does not run anything or modify existing artifacts."
    )

    context, context_warnings = _build_report_export_context()
    summary = _report_context_summary(context)
    st.code(_seed_detail_text_block(summary), language="text")

    with st.expander("Optional path overrides"):
        st.caption(
            "These are optional helpers for filling in lineage artifacts that are not already available "
            "from current session state."
        )
        st.text_input("Comparison directory", key="ui_report_comparison_dir")
        st.text_input("Review packet directory", key="ui_report_packet_dir")
        st.text_input("Candidate run directory", key="ui_report_candidate_run_dir")
        st.text_input("Outcome directory", key="ui_report_outcome_dir")
        st.text_input("Accepted baseline directory", key="ui_report_baseline_dir")

    if context_warnings:
        with st.expander(f"Context warnings ({len(context_warnings)})"):
            for warning_message in context_warnings:
                st.warning(warning_message)

    with st.form("ui_report_bundle_form", clear_on_submit=False):
        st.text_input("Report ID", key="ui_report_id")
        st.text_input("Reviewer", key="ui_report_reviewer")
        st.text_area("Notes / summary", key="ui_report_notes", height=120)
        option_columns = st.columns(2)
        with option_columns[0]:
            st.checkbox(
                "Include raw copied artifacts",
                key="ui_report_include_raw_artifacts",
            )
        with option_columns[1]:
            st.checkbox(
                "Include Markdown summary",
                key="ui_report_include_markdown_summary",
            )
        submitted = st.form_submit_button("Export Bundle", type="primary")

    if submitted:
        _submit_report_bundle_export()

    report_error = _optional_str(st.session_state.get("ui_report_error"))
    if report_error is not None:
        st.error(report_error)

    report_result = st.session_state.get("ui_report_result")
    if isinstance(report_result, ReportBundleExportResult):
        st.success(f"Exported report bundle to `{_display_path(report_result.report_dir)}`")
        st.code(
            _seed_detail_text_block(
                {
                    "report_id": report_result.report_id,
                    "report_dir": _display_path(report_result.report_dir) or "n/a",
                    "report_manifest": _display_path(report_result.manifest_path) or "n/a",
                    "context_snapshot": _display_path(report_result.context_snapshot_path) or "n/a",
                    "included_artifacts": _display_path(report_result.included_artifacts_path) or "n/a",
                    "report_summary_md": _display_path(report_result.summary_path) or "n/a",
                    "artifacts_dir": _display_path(report_result.artifacts_dir) or "n/a",
                }
            ),
            language="text",
        )
        if report_result.warnings:
            with st.expander(f"Export warnings ({len(report_result.warnings)})"):
                for warning_message in report_result.warnings:
                    st.warning(warning_message)


def _build_report_export_context(
    session_state: MutableMapping[str, Any] | None = None,
) -> tuple[dict[str, ReportArtifactContext | None], list[str]]:
    state = st.session_state if session_state is None else session_state
    context: dict[str, ReportArtifactContext | None] = {
        artifact_type: None for artifact_type in EXPECTED_REPORT_ARTIFACT_TYPES
    }
    warnings: list[str] = []

    primary_bundle = state.get("ui_loaded_batch")
    if isinstance(primary_bundle, BatchUiBundle):
        context["primary_batch"] = build_batch_report_context(role="primary", bundle=primary_bundle)

    secondary_bundle = state.get("ui_comparison_secondary_bundle")
    if isinstance(secondary_bundle, BatchUiBundle):
        context["secondary_batch"] = build_batch_report_context(role="secondary", bundle=secondary_bundle)

    comparison_dir = _first_nonempty_report_path(
        state.get("ui_report_comparison_dir"),
        getattr(state.get("ui_comparison_save_result"), "comparison_dir", None),
    )
    if comparison_dir is not None:
        context["comparison"] = _try_build_report_context(
            lambda: build_comparison_report_context(comparison_dir),
            warnings=warnings,
        )

    packet_dir = _first_nonempty_report_path(
        state.get("ui_report_packet_dir"),
        getattr(state.get("ui_loaded_reply_packet"), "packet_dir", None),
        getattr(state.get("ui_review_packet_result"), "packet_dir", None),
    )
    if packet_dir is not None:
        context["review_packet"] = _try_build_report_context(
            lambda: build_review_packet_report_context(packet_dir),
            warnings=warnings,
        )

    candidate_run_dir = _first_nonempty_report_path(
        state.get("ui_report_candidate_run_dir"),
        getattr(state.get("ui_candidate_apply_result"), "candidate_dir", None),
    )
    if candidate_run_dir is not None:
        context["candidate_run"] = _try_build_report_context(
            lambda: build_candidate_run_report_context(candidate_run_dir),
            warnings=warnings,
        )

    outcome_dir = _first_nonempty_report_path(
        state.get("ui_report_outcome_dir"),
        getattr(state.get("ui_reeval_result"), "outcome_dir", None),
    )
    if outcome_dir is not None:
        context["reeval_outcome"] = _try_build_report_context(
            lambda: build_reeval_outcome_report_context(outcome_dir),
            warnings=warnings,
        )

    baseline_dir = _first_nonempty_report_path(
        state.get("ui_report_baseline_dir"),
        state.get("ui_selected_accepted_baseline_dir"),
        getattr(state.get("ui_promotion_result"), "baseline_dir", None),
    )
    if baseline_dir is not None:
        context["accepted_baseline"] = _try_build_report_context(
            lambda: build_accepted_baseline_report_context(baseline_dir),
            warnings=warnings,
        )

    benchmark_preset_id = _optional_str(state.get("ui_selected_benchmark_preset_id"))
    benchmark_preset_path = _optional_str(state.get("ui_selected_benchmark_preset_path"))
    if benchmark_preset_path is not None:
        context["benchmark_preset"] = _try_build_report_context(
            lambda: build_preset_report_context(
                artifact_type="benchmark_preset",
                preset_id=benchmark_preset_id,
                preset_path=benchmark_preset_path,
            ),
            warnings=warnings,
        )

    eval_preset_id = _optional_str(state.get("ui_selected_eval_preset_id"))
    eval_preset_path = _optional_str(state.get("ui_selected_eval_preset_path"))
    if eval_preset_path is not None:
        context["evaluation_preset"] = _try_build_report_context(
            lambda: build_preset_report_context(
                artifact_type="evaluation_preset",
                preset_id=eval_preset_id,
                preset_path=eval_preset_path,
            ),
            warnings=warnings,
        )

    launch_profile_id = _optional_str(state.get("ui_selected_launch_profile_id"))
    launch_profile_path = _optional_str(state.get("ui_selected_launch_profile_path"))
    if launch_profile_path is not None:
        context["launch_profile"] = _try_build_report_context(
            lambda: build_preset_report_context(
                artifact_type="launch_profile",
                preset_id=launch_profile_id,
                preset_path=launch_profile_path,
            ),
            warnings=warnings,
        )

    return context, warnings


def _report_context_summary(
    context: dict[str, ReportArtifactContext | None],
) -> dict[str, Any]:
    return {
        "primary_batch": _report_context_summary_value(
            context.get("primary_batch"),
            id_key="batch_id",
            path_key="batch_dir",
        ),
        "secondary_batch": _report_context_summary_value(
            context.get("secondary_batch"),
            id_key="batch_id",
            path_key="batch_dir",
        ),
        "comparison": _report_context_summary_value(
            context.get("comparison"),
            id_key="comparison_id",
            path_key="comparison_dir",
        ),
        "review_packet": _report_context_summary_value(
            context.get("review_packet"),
            id_key="packet_id",
            path_key="packet_dir",
        ),
        "candidate_run": _report_context_summary_value(
            context.get("candidate_run"),
            id_key="candidate_id",
            path_key="candidate_run_dir",
        ),
        "reeval_outcome": _report_context_summary_value(
            context.get("reeval_outcome"),
            id_key="outcome_id",
            path_key="outcome_dir",
        ),
        "accepted_baseline": _report_context_summary_value(
            context.get("accepted_baseline"),
            id_key="baseline_id",
            path_key="baseline_dir",
        ),
        "benchmark_preset": _report_context_summary_value(
            context.get("benchmark_preset"),
            id_key="benchmark_preset_id",
            path_key="preset_path",
        ),
        "evaluation_preset": _report_context_summary_value(
            context.get("evaluation_preset"),
            id_key="eval_preset_id",
            path_key="preset_path",
        ),
        "launch_profile": _report_context_summary_value(
            context.get("launch_profile"),
            id_key="launch_profile_id",
            path_key="preset_path",
        ),
    }


def _report_context_summary_value(
    context_entry: ReportArtifactContext | None,
    *,
    id_key: str,
    path_key: str,
) -> str:
    if context_entry is None:
        return "n/a"
    artifact_id = _optional_str(context_entry.metadata.get(id_key)) or "n/a"
    artifact_path = _display_path(context_entry.metadata.get(path_key)) or "n/a"
    return f"{artifact_id} | {artifact_path}"


def _submit_report_bundle_export() -> None:
    try:
        _validate_explicit_report_paths()
        request = build_report_export_request(
            report_id=str(st.session_state.get("ui_report_id", "")),
            reviewer=str(st.session_state.get("ui_report_reviewer", "")),
            notes=str(st.session_state.get("ui_report_notes", "")),
            include_raw_copied_artifacts=bool(st.session_state.get("ui_report_include_raw_artifacts", True)),
            include_markdown_summary=bool(st.session_state.get("ui_report_include_markdown_summary", True)),
        )
        context, _ = _build_report_export_context()
        result = export_report_bundle(
            base_dir=REPO_ROOT / "runs" / "reports",
            request=request,
            context=context,
        )
    except ReportBundleExportError as exc:
        st.session_state["ui_report_error"] = str(exc)
        st.session_state["ui_report_result"] = None
        return

    st.session_state["ui_report_error"] = None
    st.session_state["ui_report_result"] = result


def _validate_explicit_report_paths(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    path_labels = {
        "ui_report_comparison_dir": "Comparison directory",
        "ui_report_packet_dir": "Review packet directory",
        "ui_report_candidate_run_dir": "Candidate run directory",
        "ui_report_outcome_dir": "Outcome directory",
        "ui_report_baseline_dir": "Accepted baseline directory",
    }
    for key, label in path_labels.items():
        value = _optional_str(state.get(key))
        if value is None:
            continue
        resolved = _resolve_existing_directory(value, label=label)
        if resolved is None:
            raise ReportBundleExportError(f"{label} does not exist: {value}")


def _try_build_report_context(
    builder: Any,
    *,
    warnings: list[str],
) -> ReportArtifactContext | None:
    try:
        return builder()
    except ReportBundleExportError as exc:
        warnings.append(str(exc))
        return None


def _first_nonempty_report_path(*values: object) -> str | None:
    for value in values:
        text = _optional_str(_display_path(value))
        if text is not None:
            return text
    return None


def _resolve_existing_directory(value: str | Path, *, label: str) -> Path | None:
    text = _optional_str(value)
    if text is None:
        return None
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = REPO_ROOT / path
    resolved = path.resolve()
    if not resolved.exists() or not resolved.is_dir():
        return None
    return resolved


def _render_benchmark_health_tab() -> None:
    st.subheader("Benchmark Health")
    st.caption(
        "Audit whether the current primary batch and optional comparison provide meaningful "
        "evidence, or whether the benchmark looks weak, saturated, or tie-heavy."
    )

    primary_bundle = st.session_state.get("ui_loaded_batch")
    if primary_bundle is None:
        _render_no_loaded_batch_message()
        return

    secondary_bundle, selected_metric, comparison_assessment = _build_comparison_discriminativeness_context(
        primary_bundle
    )
    primary_assessment = evaluate_primary_batch_health(
        batch_id=primary_bundle.manifest.batch_id,
        batch_dir=primary_bundle.batch_dir,
        seed_count=primary_bundle.manifest.seed_count,
        completed_seed_count=primary_bundle.manifest.completed_seed_count,
        failed_seed_count=primary_bundle.manifest.failed_seed_count,
        metric_aggregates=primary_bundle.aggregate_summary.metric_aggregates,
        seed_rows=primary_bundle.seed_table_rows,
    )
    context_metadata = _build_benchmark_audit_context_metadata(
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        selected_metric=selected_metric,
    )

    st.code(
        _seed_detail_text_block(_benchmark_audit_context_summary(context_metadata)),
        language="text",
    )

    _render_primary_batch_health_panel(primary_assessment)

    st.subheader("Comparison Discriminativeness")
    if secondary_bundle is None:
        st.info("Load a secondary batch in the Comparison tab to audit paired discriminativeness.")
    elif comparison_assessment is None:
        st.info("Comparison discriminativeness is not available yet.")
    else:
        _render_comparison_discriminativeness_panel(comparison_assessment)

    st.subheader("Save Benchmark Audit")
    st.caption(
        "Create a copy-only benchmark audit bundle under `runs/benchmark_audits/`. "
        "This does not run anything or modify existing artifacts."
    )
    with st.form("ui_benchmark_audit_form", clear_on_submit=False):
        st.text_input("Audit ID", key="ui_benchmark_audit_id")
        st.text_input("Reviewer", key="ui_benchmark_audit_reviewer")
        st.text_area("Notes", key="ui_benchmark_audit_notes", height=120)
        st.checkbox(
            "Include Markdown summary",
            key="ui_benchmark_audit_include_markdown_summary",
        )
        audit_submitted = st.form_submit_button("Save Benchmark Audit", type="primary")

    if audit_submitted:
        _submit_benchmark_audit_export(
            primary_bundle=primary_bundle,
            context_metadata=context_metadata,
            primary_assessment=primary_assessment,
            comparison_assessment=comparison_assessment,
        )

    audit_error = _optional_str(st.session_state.get("ui_benchmark_audit_error"))
    if audit_error is not None:
        st.error(audit_error)

    audit_result = st.session_state.get("ui_benchmark_audit_result")
    if isinstance(audit_result, BenchmarkAuditExportResult):
        st.success(f"Exported benchmark audit to `{_display_path(audit_result.audit_dir)}`")
        st.code(
            _seed_detail_text_block(
                {
                    "audit_id": audit_result.audit_id,
                    "audit_dir": _display_path(audit_result.audit_dir) or "n/a",
                    "benchmark_audit_manifest_json": _display_path(audit_result.manifest_path) or "n/a",
                    "primary_batch_health_json": _display_path(audit_result.primary_batch_health_path) or "n/a",
                    "comparison_discriminativeness_json": _display_path(
                        audit_result.comparison_discriminativeness_path
                    )
                    or "n/a",
                    "benchmark_audit_report_md": _display_path(audit_result.report_path) or "n/a",
                    "seed_quality_table_jsonl": _display_path(audit_result.seed_quality_table_path) or "n/a",
                }
            ),
            language="text",
        )


def _build_comparison_discriminativeness_context(
    primary_bundle: BatchUiBundle,
) -> tuple[BatchUiBundle | None, str | None, ComparisonDiscriminativenessAssessment | None]:
    secondary_bundle = st.session_state.get("ui_comparison_secondary_bundle")
    if not isinstance(secondary_bundle, BatchUiBundle):
        return None, None, None

    aligned_rows = align_common_seed_rows(
        primary_bundle.seed_table_rows,
        secondary_bundle.seed_table_rows,
    )
    common_doi_count = len(aligned_rows)
    common_completed_count = common_completed_seed_count(aligned_rows)
    common_metrics = common_numeric_metrics(aligned_rows)
    selected_metric = _optional_str(st.session_state.get("ui_comparison_metric"))
    if selected_metric not in common_metrics:
        selected_metric = choose_default_comparison_metric(
            primary_bundle.aggregate_summary.ranking_metric,
            common_metrics,
        )
    paired_rows = (
        paired_metric_rows(
            aligned_rows,
            metric_name=selected_metric,
            status_mode=COMPARISON_STATUS_OPTIONS[0],
        )
        if selected_metric is not None
        else []
    )
    summary = comparison_metric_summary(paired_rows) if paired_rows else None
    assessment = evaluate_comparison_discriminativeness(
        selected_metric=selected_metric,
        common_doi_count=common_doi_count,
        common_completed_seed_count=common_completed_count,
        paired_rows=paired_rows,
        summary=summary,
    )
    return secondary_bundle, selected_metric, assessment


def _build_benchmark_audit_context_metadata(
    *,
    primary_bundle: BatchUiBundle,
    secondary_bundle: BatchUiBundle | None,
    selected_metric: str | None,
    session_state: MutableMapping[str, Any] | None = None,
) -> dict[str, Any]:
    state = st.session_state if session_state is None else session_state
    selected_eval_values = state.get("ui_selected_eval_preset_values")
    selected_launch_profile_values = state.get("ui_selected_launch_profile_values")

    context: dict[str, Any] = {
        "primary_batch": {
            "batch_id": primary_bundle.manifest.batch_id,
            "batch_dir": _display_path(primary_bundle.batch_dir) or None,
            "theory_config": _display_path(primary_bundle.manifest.theory_config) or None,
            "seed_count": primary_bundle.manifest.seed_count,
            "completed_seed_count": primary_bundle.manifest.completed_seed_count,
            "failed_seed_count": primary_bundle.manifest.failed_seed_count,
            "ranking_metric": primary_bundle.aggregate_summary.ranking_metric,
        },
        "secondary_batch": None,
        "selected_metric": selected_metric,
        "accepted_baseline": None,
        "benchmark_preset": None,
        "evaluation_preset": None,
        "launch_profile": None,
    }
    if secondary_bundle is not None:
        context["secondary_batch"] = {
            "batch_id": secondary_bundle.manifest.batch_id,
            "batch_dir": _display_path(secondary_bundle.batch_dir) or None,
            "theory_config": _display_path(secondary_bundle.manifest.theory_config) or None,
            "seed_count": secondary_bundle.manifest.seed_count,
            "completed_seed_count": secondary_bundle.manifest.completed_seed_count,
            "failed_seed_count": secondary_bundle.manifest.failed_seed_count,
            "ranking_metric": secondary_bundle.aggregate_summary.ranking_metric,
        }

    accepted_baseline_id = _optional_str(state.get("ui_selected_accepted_baseline_id"))
    if accepted_baseline_id is not None:
        context["accepted_baseline"] = {
            "baseline_id": accepted_baseline_id,
            "baseline_dir": _optional_str(state.get("ui_selected_accepted_baseline_dir")),
            "accepted_theory_snapshot_path": _optional_str(state.get("ui_selected_accepted_baseline_theory_path")),
        }

    benchmark_preset_id = _optional_str(state.get("ui_selected_benchmark_preset_id"))
    if benchmark_preset_id is not None:
        context["benchmark_preset"] = {
            "benchmark_preset_id": benchmark_preset_id,
            "preset_path": _optional_str(state.get("ui_selected_benchmark_preset_path")),
            "seeds_csv": _optional_str(state.get("ui_selected_benchmark_preset_seeds_csv")),
        }

    eval_preset_id = _optional_str(state.get("ui_selected_eval_preset_id"))
    if eval_preset_id is not None:
        context["evaluation_preset"] = {
            "eval_preset_id": eval_preset_id,
            "preset_path": _optional_str(state.get("ui_selected_eval_preset_path")),
            **(dict(selected_eval_values) if isinstance(selected_eval_values, dict) else {}),
        }

    launch_profile_id = _optional_str(state.get("ui_selected_launch_profile_id"))
    if launch_profile_id is not None:
        context["launch_profile"] = {
            "launch_profile_id": launch_profile_id,
            "preset_path": _optional_str(state.get("ui_selected_launch_profile_path")),
            **(dict(selected_launch_profile_values) if isinstance(selected_launch_profile_values, dict) else {}),
        }

    return context


def _benchmark_audit_context_summary(
    context_metadata: dict[str, Any],
) -> dict[str, Any]:
    primary_batch = context_metadata.get("primary_batch") or {}
    secondary_batch = context_metadata.get("secondary_batch") or {}
    accepted_baseline = context_metadata.get("accepted_baseline") or {}
    benchmark_preset = context_metadata.get("benchmark_preset") or {}
    evaluation_preset = context_metadata.get("evaluation_preset") or {}
    launch_profile = context_metadata.get("launch_profile") or {}
    return {
        "primary_batch": _context_summary_line(primary_batch, id_key="batch_id", path_key="batch_dir"),
        "secondary_batch": _context_summary_line(secondary_batch, id_key="batch_id", path_key="batch_dir"),
        "selected_metric": _metric_label(_optional_str(context_metadata.get("selected_metric"))),
        "accepted_baseline": _context_summary_line(
            accepted_baseline,
            id_key="baseline_id",
            path_key="baseline_dir",
        ),
        "benchmark_preset": _context_summary_line(
            benchmark_preset,
            id_key="benchmark_preset_id",
            path_key="preset_path",
        ),
        "evaluation_preset": _context_summary_line(
            evaluation_preset,
            id_key="eval_preset_id",
            path_key="preset_path",
        ),
        "launch_profile": _context_summary_line(
            launch_profile,
            id_key="launch_profile_id",
            path_key="preset_path",
        ),
    }


def _context_summary_line(
    metadata: dict[str, Any],
    *,
    id_key: str,
    path_key: str,
) -> str:
    artifact_id = _optional_str(metadata.get(id_key)) or "n/a"
    artifact_path = _display_path(metadata.get(path_key)) or "n/a"
    return f"{artifact_id} | {artifact_path}"


def _render_primary_batch_health_panel(
    assessment: PrimaryBatchHealthAssessment,
) -> None:
    st.subheader("Primary Batch Health")
    _render_health_verdict_message(
        verdict=assessment.verdict,
        message=f"Primary batch health verdict: `{assessment.verdict}`.",
    )
    summary_columns = st.columns(5)
    summary_columns[0].metric("Seeds", assessment.seed_count)
    summary_columns[1].metric("Completed", assessment.completed_seed_count)
    summary_columns[2].metric("Failed", assessment.failed_seed_count)
    summary_columns[3].metric("Failure Rate", assessment.failure_rate if assessment.failure_rate is not None else "n/a")
    summary_columns[4].metric(
        "Completed w/ Numeric",
        assessment.completed_seeds_with_numeric_metrics,
    )
    st.caption(
        "Available metrics: "
        + (", ".join(_metric_label(metric_name) for metric_name in assessment.available_metrics) if assessment.available_metrics else "n/a")
    )

    metric_rows = [
        {
            "metric": _metric_label(metric.metric_name),
            "count": metric.count,
            "missing_count": metric.missing_count,
            "mean": metric.mean,
            "median": metric.median,
            "std": metric.std,
            "saturation_rate": metric.saturation_rate,
        }
        for metric in assessment.metric_summaries
        if metric.count > 0
    ]
    if metric_rows:
        st.dataframe(
            _rows_to_table_data(metric_rows),
            use_container_width=True,
            hide_index=True,
        )
    for reason in assessment.reasons:
        st.markdown(f"- {reason}")


def _render_comparison_discriminativeness_panel(
    assessment: ComparisonDiscriminativenessAssessment,
) -> None:
    _render_health_verdict_message(
        verdict=assessment.verdict,
        message=(
            "Comparison discriminativeness verdict: "
            f"`{assessment.verdict}` on `{_metric_label(assessment.selected_metric)}`."
        ),
    )
    summary_columns = st.columns(6)
    summary_columns[0].metric("Metric", _metric_label(assessment.selected_metric))
    summary_columns[1].metric("Common DOIs", assessment.common_doi_count)
    summary_columns[2].metric("Common Completed", assessment.common_completed_seed_count)
    summary_columns[3].metric("Wins", assessment.wins)
    summary_columns[4].metric("Losses", assessment.losses)
    summary_columns[5].metric("Ties", assessment.ties)
    detail_columns = st.columns(3)
    detail_columns[0].metric("Tie Rate", assessment.tie_rate if assessment.tie_rate is not None else "n/a")
    detail_columns[1].metric(
        "Near-Zero Delta Fraction",
        assessment.near_zero_delta_fraction if assessment.near_zero_delta_fraction is not None else "n/a",
    )
    detail_columns[2].metric(
        "Improvement Delta Std",
        assessment.improvement_delta_std if assessment.improvement_delta_std is not None else "n/a",
    )
    st.caption(
        "Improvement delta mean / median: "
        f"`{assessment.improvement_delta_mean}` / `{assessment.improvement_delta_median}`"
    )
    for reason in assessment.reasons:
        st.markdown(f"- {reason}")


def _render_health_verdict_message(*, verdict: str, message: str) -> None:
    if verdict == "usable":
        st.success(message)
    elif verdict == "weak":
        st.warning(message)
    else:
        st.error(message)


def _submit_benchmark_audit_export(
    *,
    primary_bundle: BatchUiBundle,
    context_metadata: dict[str, Any],
    primary_assessment: PrimaryBatchHealthAssessment,
    comparison_assessment: ComparisonDiscriminativenessAssessment | None,
) -> None:
    try:
        request = build_benchmark_audit_export_request(
            audit_id=str(st.session_state.get("ui_benchmark_audit_id", "")),
            reviewer=str(st.session_state.get("ui_benchmark_audit_reviewer", "")),
            notes=str(st.session_state.get("ui_benchmark_audit_notes", "")),
            include_markdown_summary=bool(
                st.session_state.get("ui_benchmark_audit_include_markdown_summary", True)
            ),
        )
        result = export_benchmark_audit(
            base_dir=REPO_ROOT / "runs" / "benchmark_audits",
            request=request,
            context_metadata=context_metadata,
            primary_batch_health=primary_assessment,
            comparison_discriminativeness=comparison_assessment,
            seed_quality_rows=build_seed_quality_rows(primary_bundle.seed_table_rows),
        )
    except BenchmarkAuditError as exc:
        st.session_state["ui_benchmark_audit_error"] = str(exc)
        st.session_state["ui_benchmark_audit_result"] = None
        return

    st.session_state["ui_benchmark_audit_error"] = None
    st.session_state["ui_benchmark_audit_result"] = result


def _render_benchmark_curation_tab() -> None:
    st.subheader("Benchmark Curation")
    st.caption(
        "Review seed-level benchmark quality signals and export a curated seed set for future "
        "batch runs. This is explicit, read-only with respect to source artifacts, and copy-only on export."
    )

    primary_bundle = st.session_state.get("ui_loaded_batch")
    if primary_bundle is None:
        _render_no_loaded_batch_message()
        return
    if not primary_bundle.seed_table_rows:
        st.info("The loaded primary batch does not contain any seed rows to curate.")
        return

    primary_assessment = evaluate_primary_batch_health(
        batch_id=primary_bundle.manifest.batch_id,
        batch_dir=primary_bundle.batch_dir,
        seed_count=primary_bundle.manifest.seed_count,
        completed_seed_count=primary_bundle.manifest.completed_seed_count,
        failed_seed_count=primary_bundle.manifest.failed_seed_count,
        metric_aggregates=primary_bundle.aggregate_summary.metric_aggregates,
        seed_rows=primary_bundle.seed_table_rows,
    )
    secondary_bundle, selected_metric, comparison_paired_rows, comparison_assessment = _build_curation_comparison_context(
        primary_bundle
    )
    curation_rows = build_seed_curation_rows(
        primary_bundle.seed_table_rows,
        comparison_rows_by_doi={
            str(row.get("doi")): row
            for row in comparison_paired_rows
            if row.get("doi") not in (None, "")
        },
        selected_metric=selected_metric,
    )
    _sync_curation_preset_dir_before_render()
    _sync_benchmark_curation_decisions(
        batch_id=primary_bundle.manifest.batch_id,
        seed_rows=curation_rows,
    )
    curation_decisions = st.session_state.get("ui_curation_decisions")
    if not isinstance(curation_decisions, dict):
        curation_decisions = normalize_curation_decisions(curation_rows, None)
        st.session_state["ui_curation_decisions"] = curation_decisions

    context_metadata = _build_benchmark_audit_context_metadata(
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        selected_metric=selected_metric,
    )
    st.code(
        _seed_detail_text_block(
            {
                **_benchmark_audit_context_summary(context_metadata),
                "current_benchmark_audit": _current_benchmark_audit_summary(
                    primary_assessment=primary_assessment,
                    comparison_assessment=comparison_assessment,
                ),
            }
        ),
        language="text",
    )

    curation_summary = summarize_curation_decisions(
        curation_rows,
        curation_decisions,
        comparison_context_used=comparison_assessment is not None,
    )
    summary_columns = st.columns(7)
    summary_columns[0].metric("Total Seeds", curation_summary["total_seeds"])
    summary_columns[1].metric("Keep", curation_summary["keep_count"])
    summary_columns[2].metric("Review", curation_summary["review_count"])
    summary_columns[3].metric("Exclude", curation_summary["exclude_count"])
    summary_columns[4].metric("Failed", curation_summary["failed_seed_count"])
    summary_columns[5].metric("Usable Completed", curation_summary["usable_completed_seed_count"])
    summary_columns[6].metric(
        "Comparison Context",
        "Yes" if curation_summary["comparison_context_used"] else "No",
    )

    filter_columns = st.columns([1.2, 1.0, 1.0, 1.0, 1.6])
    with filter_columns[0]:
        st.selectbox(
            "Decision filter",
            options=list(CURATION_DECISION_FILTER_OPTIONS),
            key="ui_curation_decision_filter",
        )
    with filter_columns[1]:
        st.checkbox("Only failed", key="ui_curation_only_failed")
    with filter_columns[2]:
        st.checkbox("Only saturated", key="ui_curation_only_saturated")
    with filter_columns[3]:
        st.checkbox(
            "Only tie-like",
            key="ui_curation_only_tie_like",
            disabled=comparison_assessment is None,
        )
    with filter_columns[4]:
        st.text_input("DOI search", key="ui_curation_doi_filter")

    filtered_rows = filter_curation_rows(
        curation_rows,
        curation_decisions,
        decision_filter=str(st.session_state.get("ui_curation_decision_filter", "all")),
        only_failed=bool(st.session_state.get("ui_curation_only_failed", False)),
        only_saturated=bool(st.session_state.get("ui_curation_only_saturated", False)),
        only_tie_like=bool(st.session_state.get("ui_curation_only_tie_like", False)),
        doi_filter=str(st.session_state.get("ui_curation_doi_filter", "")),
    )

    st.markdown("**Seed-Level Curation Table**")
    if not filtered_rows:
        st.info("No seed rows match the current curation filters.")
    else:
        st.dataframe(
            _rows_to_table_data(
                [
                    {
                        "batch_index": row.get("batch_index"),
                        "doi": row.get("doi"),
                        "status": row.get("status"),
                        "decision": row.get("decision"),
                        "suggested_decision": row.get("suggested_decision"),
                        "reason_summary": row.get("reason_summary"),
                        "precision_at_k": row.get("precision_at_k"),
                        "recall_at_k": row.get("recall_at_k"),
                        "ndcg_at_k": row.get("ndcg_at_k"),
                        "brier_score": row.get("brier_score"),
                        "expected_calibration_error": row.get("expected_calibration_error"),
                        "failed_seed": row.get("failed_seed"),
                        "missing_metrics": row.get("missing_metrics"),
                        "saturated_ndcg": row.get("saturated_ndcg"),
                        "saturated_precision": row.get("saturated_precision"),
                        "low_signal_seed": row.get("low_signal_seed"),
                        "improvement_delta": row.get("improvement_delta"),
                        "tie_like_seed": row.get("tie_like_seed"),
                        "error_summary": row.get("error_summary"),
                    }
                    for row in filtered_rows
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("**Manual Override**")
    st.caption("Suggested decisions are only defaults. Export uses the current explicit decisions shown here.")
    visible_row_lookup = {
        str(row.get("doi")): row
        for row in filtered_rows
        if row.get("doi") not in (None, "")
    }
    with st.form("ui_curation_override_form", clear_on_submit=False):
        st.multiselect(
            "Seeds to update",
            options=list(visible_row_lookup.keys()),
            key="ui_curation_selected_dois",
            format_func=lambda doi: _curation_seed_option_label(
                visible_row_lookup.get(doi) or {},
                str(curation_decisions.get(doi, "")),
            ),
        )
        st.selectbox(
            "Set decision",
            options=list(CURATION_DECISION_OPTIONS),
            key="ui_curation_target_decision",
        )
        override_submitted = st.form_submit_button(
            "Apply Decision",
            type="primary",
            disabled=not bool(visible_row_lookup),
        )

    if override_submitted:
        selected_dois = [
            doi
            for doi in st.session_state.get("ui_curation_selected_dois", [])
            if doi in visible_row_lookup
        ]
        if not selected_dois:
            st.session_state["ui_curation_error"] = "Select at least one visible seed to update its decision."
        else:
            updated_decisions = dict(curation_decisions)
            target_decision = str(st.session_state.get("ui_curation_target_decision", CURATION_DECISION_OPTIONS[1]))
            for doi in selected_dois:
                updated_decisions[doi] = target_decision
            st.session_state["ui_curation_decisions"] = updated_decisions
            st.session_state["ui_curation_error"] = None
            st.rerun()

    st.subheader("Export Curation")
    with st.form("ui_benchmark_curation_export_form", clear_on_submit=False):
        st.text_input("Curation ID", key="ui_curation_id")
        st.text_input("Reviewer", key="ui_curation_reviewer")
        st.text_area("Notes", key="ui_curation_notes", height=120)
        option_columns = st.columns(3)
        with option_columns[0]:
            st.checkbox(
                "Export only kept seeds to CSV",
                key="ui_curation_export_only_kept",
            )
        with option_columns[1]:
            st.checkbox(
                "Include review seeds in a separate CSV",
                key="ui_curation_include_review_csv",
            )
        with option_columns[2]:
            st.checkbox(
                "Include Markdown summary",
                key="ui_curation_include_markdown_summary",
            )
        export_submitted = st.form_submit_button("Export Curation", type="primary")

    if export_submitted:
        _submit_benchmark_curation_export(
            primary_bundle=primary_bundle,
            secondary_bundle=secondary_bundle,
            selected_metric=selected_metric,
            primary_assessment=primary_assessment,
            comparison_assessment=comparison_assessment,
            seed_rows=curation_rows,
        )

    curation_error = _optional_str(st.session_state.get("ui_curation_error"))
    if curation_error is not None:
        st.error(curation_error)

    curation_result = st.session_state.get("ui_curation_result")
    if isinstance(curation_result, BenchmarkCurationExportResult):
        st.success(f"Exported benchmark curation to `{_display_path(curation_result.curation_dir)}`")
        st.code(
            _seed_detail_text_block(
                {
                    "curation_id": curation_result.curation_id,
                    "curation_dir": _display_path(curation_result.curation_dir) or "n/a",
                    "curation_manifest_json": _display_path(curation_result.manifest_path) or "n/a",
                    "seed_decisions_jsonl": _display_path(curation_result.seed_decisions_path) or "n/a",
                    "curated_seeds_csv": _display_path(curation_result.curated_seeds_csv_path) or "n/a",
                    "review_seeds_csv": _display_path(curation_result.review_seeds_csv_path) or "n/a",
                    "curation_report_md": _display_path(curation_result.report_path) or "n/a",
                }
            ),
            language="text",
        )
        st.caption(
            "The curated CSV is a copy-only artifact and can be used later as a benchmark preset source if you want."
        )

    st.subheader("Save as Benchmark Preset")
    st.caption(
        "Create a normal benchmark preset JSON that points at the curated `curated_seeds.csv` output. "
        "This stays copy-only and does not modify the curation bundle."
    )
    curation_dir_value = _optional_str(st.session_state.get("ui_curation_preset_dir"))
    if curation_dir_value is not None:
        curation_bundle, curation_bundle_error = _load_curation_bundle_for_display(curation_dir_value)
        if curation_bundle_error is not None:
            st.warning(curation_bundle_error)
        elif curation_bundle is not None:
            st.code(
                _seed_detail_text_block(
                    {
                        "curation_id": curation_bundle.curation_id,
                        "curation_dir": _display_path(curation_bundle.curation_dir) or "n/a",
                        "curated_seeds_csv": _display_path(curation_bundle.curated_seeds_csv_path) or "n/a",
                        "curated_seed_count": curation_bundle.curated_seed_count,
                        "review_seed_count": curation_bundle.review_seed_count,
                        "source_primary_batch_id": (
                            curation_bundle.manifest.get("primary_batch", {}).get("batch_id")
                            if isinstance(curation_bundle.manifest.get("primary_batch"), dict)
                            else "n/a"
                        ),
                        "source_benchmark_preset_id": (
                            curation_bundle.manifest.get("source_benchmark_preset", {}).get("benchmark_preset_id")
                            if isinstance(curation_bundle.manifest.get("source_benchmark_preset"), dict)
                            else "n/a"
                        ),
                    }
                ),
                language="text",
            )

    with st.form("ui_curation_benchmark_preset_form", clear_on_submit=False):
        st.text_input("Benchmark Preset ID", key="ui_curation_preset_id")
        st.text_input("Curation Directory", key="ui_curation_preset_dir")
        st.text_input("Description / notes", key="ui_curation_preset_description")
        st.text_input("Tags (comma-separated)", key="ui_curation_preset_tags")
        st.checkbox(
            "Also select this benchmark preset for Run Batch",
            key="ui_curation_preset_select_for_run_batch",
        )
        save_curated_preset_submitted = st.form_submit_button(
            "Save as Benchmark Preset",
            type="primary",
        )

    if save_curated_preset_submitted:
        _submit_save_curated_benchmark_preset()

    curation_preset_error = _optional_str(st.session_state.get("ui_curation_preset_error"))
    if curation_preset_error is not None:
        st.error(curation_preset_error)
    curation_preset_saved_path = _optional_str(st.session_state.get("ui_curation_preset_saved_path"))
    if curation_preset_saved_path is not None:
        st.success(f"Saved benchmark preset to `{curation_preset_saved_path}`")


def _build_curation_comparison_context(
    primary_bundle: BatchUiBundle,
) -> tuple[
    BatchUiBundle | None,
    str | None,
    list[dict[str, Any]],
    ComparisonDiscriminativenessAssessment | None,
]:
    secondary_bundle = st.session_state.get("ui_comparison_secondary_bundle")
    if not isinstance(secondary_bundle, BatchUiBundle):
        return None, None, [], None

    aligned_rows = align_common_seed_rows(
        primary_bundle.seed_table_rows,
        secondary_bundle.seed_table_rows,
    )
    common_metrics = common_numeric_metrics(aligned_rows)
    selected_metric = _optional_str(st.session_state.get("ui_comparison_metric"))
    if selected_metric not in common_metrics:
        selected_metric = choose_default_comparison_metric(
            primary_bundle.aggregate_summary.ranking_metric,
            common_metrics,
        )
    if selected_metric is None:
        return secondary_bundle, None, [], None

    paired_rows = paired_metric_rows(
        aligned_rows,
        metric_name=selected_metric,
        status_mode=COMPARISON_STATUS_OPTIONS[0],
    )
    assessment = evaluate_comparison_discriminativeness(
        selected_metric=selected_metric,
        common_doi_count=len(aligned_rows),
        common_completed_seed_count=common_completed_seed_count(aligned_rows),
        paired_rows=paired_rows,
        summary=(comparison_metric_summary(paired_rows) if paired_rows else None),
    )
    return secondary_bundle, selected_metric, paired_rows, assessment


def _sync_curation_preset_dir_before_render(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    if _optional_str(state.get("ui_curation_preset_dir")) is not None:
        return
    latest_result = state.get("ui_curation_result")
    if isinstance(latest_result, BenchmarkCurationExportResult):
        state["ui_curation_preset_dir"] = _display_path(latest_result.curation_dir)


def _load_curation_bundle_for_display(
    curation_dir: str,
) -> tuple[Any | None, str | None]:
    if not curation_dir.strip():
        return None, None
    try:
        return load_curation_bundle_context(curation_dir), None
    except CuratedBenchmarkPresetError as exc:
        return None, str(exc)


def _sync_benchmark_curation_decisions(
    *,
    batch_id: str,
    seed_rows: list[dict[str, Any]],
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    if _optional_str(state.get("ui_curation_source_batch_id")) != _optional_str(batch_id):
        state["ui_curation_source_batch_id"] = batch_id
        state["ui_curation_decisions"] = normalize_curation_decisions(seed_rows, None)
        return

    current_decisions = state.get("ui_curation_decisions")
    if not isinstance(current_decisions, dict):
        state["ui_curation_decisions"] = normalize_curation_decisions(seed_rows, None)
        return
    state["ui_curation_decisions"] = normalize_curation_decisions(seed_rows, current_decisions)


def _current_benchmark_audit_summary(
    *,
    primary_assessment: PrimaryBatchHealthAssessment,
    comparison_assessment: ComparisonDiscriminativenessAssessment | None,
) -> str:
    if comparison_assessment is None:
        return f"primary={primary_assessment.verdict}"
    return (
        f"primary={primary_assessment.verdict}; "
        f"comparison={comparison_assessment.verdict}"
    )


def _curation_seed_option_label(seed_row: dict[str, Any], current_decision: str) -> str:
    doi = seed_row.get("doi") or "unknown"
    status = seed_row.get("status") or "unknown"
    suggestion = seed_row.get("suggested_decision") or "n/a"
    decision = current_decision or suggestion
    return f"{doi} [{status}] current={decision} suggested={suggestion}"


def _submit_benchmark_curation_export(
    *,
    primary_bundle: BatchUiBundle,
    secondary_bundle: BatchUiBundle | None,
    selected_metric: str | None,
    primary_assessment: PrimaryBatchHealthAssessment,
    comparison_assessment: ComparisonDiscriminativenessAssessment | None,
    seed_rows: list[dict[str, Any]],
) -> None:
    try:
        request = build_benchmark_curation_export_request(
            curation_id=str(st.session_state.get("ui_curation_id", "")),
            reviewer=str(st.session_state.get("ui_curation_reviewer", "")),
            notes=str(st.session_state.get("ui_curation_notes", "")),
            export_only_kept_to_csv=bool(st.session_state.get("ui_curation_export_only_kept", True)),
            include_review_seeds_csv=bool(st.session_state.get("ui_curation_include_review_csv", True)),
            include_markdown_summary=bool(st.session_state.get("ui_curation_include_markdown_summary", True)),
        )
        decisions = st.session_state.get("ui_curation_decisions")
        if not isinstance(decisions, dict):
            decisions = normalize_curation_decisions(seed_rows, None)
        context_metadata = {
            "primary_batch": {
                "batch_id": primary_bundle.manifest.batch_id,
                "batch_dir": _display_path(primary_bundle.batch_dir) or None,
                "theory_config": _display_path(primary_bundle.manifest.theory_config) or None,
                "seeds_csv": _display_path(primary_bundle.manifest.seeds_csv) or None,
            },
            "secondary_batch": (
                {
                    "batch_id": secondary_bundle.manifest.batch_id,
                    "batch_dir": _display_path(secondary_bundle.batch_dir) or None,
                    "theory_config": _display_path(secondary_bundle.manifest.theory_config) or None,
                }
                if secondary_bundle is not None
                else None
            ),
            "selected_comparison_metric": selected_metric,
            "source_benchmark_preset": (
                {
                    "benchmark_preset_id": _optional_str(st.session_state.get("ui_selected_benchmark_preset_id")),
                    "preset_path": _optional_str(st.session_state.get("ui_selected_benchmark_preset_path")),
                    "seeds_csv": _optional_str(st.session_state.get("ui_selected_benchmark_preset_seeds_csv")),
                }
                if _optional_str(st.session_state.get("ui_selected_benchmark_preset_id")) is not None
                else None
            ),
            "benchmark_health": {
                "primary_batch_health_verdict": primary_assessment.verdict,
                "comparison_discriminativeness_verdict": (
                    comparison_assessment.verdict if comparison_assessment is not None else None
                ),
            },
        }
        result = export_benchmark_curation(
            base_dir=REPO_ROOT / "runs" / "benchmark_curations",
            request=request,
            context_metadata=context_metadata,
            seed_rows=seed_rows,
            decisions=decisions,
        )
    except BenchmarkCurationError as exc:
        st.session_state["ui_curation_error"] = str(exc)
        st.session_state["ui_curation_result"] = None
        return

    st.session_state["ui_curation_error"] = None
    st.session_state["ui_curation_result"] = result


def _render_launch_profiles_section() -> None:
    st.markdown("**Create Launch Profile**")
    st.caption(
        "Launch profiles bundle the currently selected accepted baseline, benchmark preset, "
        "and evaluation preset into one reusable local launch configuration."
    )

    current_context = _current_launch_profile_selection_context()
    st.code(
        _seed_detail_text_block(
            {
                "accepted_baseline_id": current_context.get("accepted_baseline_id") or "n/a",
                "accepted_theory_snapshot": _display_path(current_context.get("accepted_theory_snapshot")) or "n/a",
                "benchmark_preset_id": current_context.get("benchmark_preset_id") or "n/a",
                "seeds_csv": _display_path(current_context.get("seeds_csv")) or "n/a",
                "eval_preset_id": current_context.get("eval_preset_id") or "n/a",
                "label_source": current_context.get("label_source") or "n/a",
            }
        ),
        language="text",
    )

    with st.form("ui_launch_profile_form", clear_on_submit=False):
        st.text_input("Launch Profile ID", key="ui_new_launch_profile_id")
        st.text_input("Description / notes", key="ui_new_launch_profile_description")
        st.text_input("Tags (comma-separated)", key="ui_new_launch_profile_tags")
        launch_profile_submitted = st.form_submit_button(
            "Save Launch Profile",
            type="primary",
        )

    if launch_profile_submitted:
        _submit_save_launch_profile()

    launch_profile_error = st.session_state.get("ui_launch_profile_error")
    if launch_profile_error:
        st.error(launch_profile_error)
    launch_profile_saved_path = _optional_str(st.session_state.get("ui_launch_profile_saved_path"))
    if launch_profile_saved_path is not None:
        st.success(f"Saved launch profile to `{launch_profile_saved_path}`")

    entries, warnings = scan_launch_profiles()
    if warnings:
        with st.expander(f"Launch profile warnings ({len(warnings)})"):
            for warning_message in warnings:
                st.warning(warning_message)

    if not entries:
        st.info("No launch profiles were found yet.")
        return

    st.dataframe(
        _rows_to_table_data(build_launch_profile_rows(entries)),
        use_container_width=True,
        hide_index=True,
    )

    default_profile_id = choose_default_launch_profile_id(
        entries,
        preferred_profile_id=(
            _optional_str(st.session_state.get("ui_registry_selected_launch_profile_id"))
            or _optional_str(st.session_state.get("ui_selected_launch_profile_id"))
        ),
    )
    if default_profile_id is None:
        return
    if st.session_state.get("ui_registry_selected_launch_profile_id") not in {
        entry.profile_id for entry in entries
    }:
        st.session_state["ui_registry_selected_launch_profile_id"] = default_profile_id

    st.selectbox(
        "Launch profile",
        options=[entry.profile_id for entry in entries],
        key="ui_registry_selected_launch_profile_id",
    )
    selected_entry = find_launch_profile_entry(
        entries,
        _optional_str(st.session_state.get("ui_registry_selected_launch_profile_id")),
    )
    if selected_entry is None:
        st.info("Select a launch profile to inspect it.")
        return

    current_selected_profile_id = _optional_str(st.session_state.get("ui_selected_launch_profile_id"))
    if current_selected_profile_id is not None and not any(
        entry.profile_id == current_selected_profile_id for entry in entries
    ):
        st.warning(
            "The currently selected launch profile is no longer present in the registry. "
            "You can choose another one below or clear the session-state selection."
        )
    if current_selected_profile_id == selected_entry.profile_id:
        st.success("This launch profile is currently selected for Run Batch convenience prefill.")

    detail = build_launch_profile_detail(selected_entry)
    detail_columns = st.columns(4)
    detail_columns[0].metric("Launch Profile ID", detail.get("launch_profile_id") or "n/a")
    detail_columns[1].metric("Accepted Baseline", detail.get("accepted_baseline_id") or "n/a")
    detail_columns[2].metric("Benchmark Preset", detail.get("benchmark_preset_id") or "n/a")
    detail_columns[3].metric("Evaluation Preset", detail.get("eval_preset_id") or "n/a")

    selected_values, selected_warnings = build_launch_profile_run_batch_values(
        selected_entry,
        allowed_label_sources=SUPPORTED_LABEL_SOURCES,
        fallback_label_source=SUPPORTED_LABEL_SOURCES[0],
    )
    action_columns = st.columns([1.6, 1.2, 1.2])
    with action_columns[0]:
        if st.button(
            "Use this launch profile for Run Batch",
            type="primary",
            use_container_width=True,
        ):
            _select_launch_profile_for_run_batch(
                selected_entry,
                values=selected_values,
                warnings=selected_warnings,
            )
            st.rerun()
    with action_columns[1]:
        if st.button(
            "Clear selected launch profile",
            use_container_width=True,
            disabled=not bool(current_selected_profile_id),
        ):
            _clear_selected_launch_profile()
            st.rerun()
    with action_columns[2]:
        if selected_warnings:
            st.caption("Prefill warnings available below.")

    if selected_warnings:
        for warning_message in selected_warnings:
            st.warning(warning_message)

    st.code(
        _seed_detail_text_block(
            {
                "profile_path": _display_path(detail.get("profile_path")) or "n/a",
                "accepted_theory_snapshot": _display_path(detail.get("accepted_theory_snapshot")) or "n/a",
                "seeds_csv": _display_path(detail.get("seeds_csv")) or "n/a",
                "max_references": _display_summary_value(detail.get("max_references")),
                "max_related": _display_summary_value(detail.get("max_related")),
                "max_hard_negatives": _display_summary_value(detail.get("max_hard_negatives")),
                "top_k": _display_summary_value(detail.get("top_k")),
                "label_source": detail.get("label_source") or "n/a",
                "refresh": detail.get("refresh"),
                "description": detail.get("description") or "n/a",
                "tags": ", ".join(detail.get("tags") or []) or "n/a",
            }
        ),
        language="text",
    )

    st.markdown("**Run This Profile Now**")
    st.caption("This is explicit and reuses the same Python-level Run Batch integration as the main form.")
    with st.form("ui_launch_profile_run_form", clear_on_submit=False):
        st.text_input("Output Batch ID", key="ui_launch_profile_run_batch_id")
        st.text_input(
            "Initial DOI context (optional UI-only)",
            key="ui_launch_profile_run_initial_doi_context",
            help="Optional UI-only context. It does not change backend batch semantics.",
        )
        run_profile_submitted = st.form_submit_button(
            "Run this profile now",
            type="primary",
            disabled=bool(st.session_state.get("ui_run_in_progress")),
        )

    if run_profile_submitted:
        _submit_run_launch_profile(selected_entry)

    run_error = st.session_state.get("ui_launch_profile_run_error")
    if run_error:
        st.error(run_error)
    run_warning = st.session_state.get("ui_launch_profile_run_warning")
    if run_warning:
        st.warning(run_warning)
    run_summary = st.session_state.get("ui_launch_profile_run_summary")
    if isinstance(run_summary, BatchRunSummary):
        _render_batch_run_summary(run_summary)

    with st.expander("Raw launch profile JSON"):
        st.json(detail.get("raw_payload"))


def _render_benchmark_presets_section() -> None:
    st.markdown("**Create Benchmark Preset**")
    with st.form("ui_benchmark_preset_form", clear_on_submit=False):
        st.text_input("Benchmark Preset ID", key="ui_new_benchmark_preset_id")
        st.text_input("Seeds CSV path", key="ui_new_benchmark_preset_seeds_csv")
        st.text_input("Description / notes", key="ui_new_benchmark_preset_description")
        st.text_input("Tags (comma-separated)", key="ui_new_benchmark_preset_tags")
        benchmark_submitted = st.form_submit_button("Save Benchmark Preset", type="primary")

    if benchmark_submitted:
        _submit_save_benchmark_preset()

    benchmark_error = st.session_state.get("ui_benchmark_preset_error")
    if benchmark_error:
        st.error(benchmark_error)
    benchmark_saved_path = _optional_str(st.session_state.get("ui_benchmark_preset_saved_path"))
    if benchmark_saved_path is not None:
        st.success(f"Saved benchmark preset to `{benchmark_saved_path}`")

    entries, warnings = scan_benchmark_presets()
    if warnings:
        with st.expander(f"Benchmark preset warnings ({len(warnings)})"):
            for warning_message in warnings:
                st.warning(warning_message)

    if not entries:
        st.info("No benchmark presets were found yet.")
        return

    st.dataframe(
        _rows_to_table_data(build_benchmark_preset_rows(entries)),
        use_container_width=True,
        hide_index=True,
    )

    default_preset_id = choose_default_benchmark_preset_id(
        entries,
        preferred_preset_id=(
            _optional_str(st.session_state.get("ui_registry_selected_benchmark_preset_id"))
            or _optional_str(st.session_state.get("ui_selected_benchmark_preset_id"))
        ),
    )
    if default_preset_id is None:
        return
    if st.session_state.get("ui_registry_selected_benchmark_preset_id") not in {
        entry.preset_id for entry in entries
    }:
        st.session_state["ui_registry_selected_benchmark_preset_id"] = default_preset_id

    st.selectbox(
        "Benchmark preset",
        options=[entry.preset_id for entry in entries],
        key="ui_registry_selected_benchmark_preset_id",
    )
    selected_entry = find_benchmark_preset_entry(
        entries,
        _optional_str(st.session_state.get("ui_registry_selected_benchmark_preset_id")),
    )
    if selected_entry is None:
        st.info("Select a benchmark preset to inspect it.")
        return

    current_selected_id = _optional_str(st.session_state.get("ui_selected_benchmark_preset_id"))
    if current_selected_id is not None and not any(entry.preset_id == current_selected_id for entry in entries):
        st.warning(
            "The currently selected benchmark preset is no longer present in the registry. "
            "You can choose another one below or clear the session-state selection."
        )
    if current_selected_id == selected_entry.preset_id:
        st.success("This benchmark preset is currently selected for Run Batch convenience prefill.")

    detail = build_benchmark_preset_detail(selected_entry)
    detail_columns = st.columns(4)
    detail_columns[0].metric("Preset ID", detail.get("benchmark_preset_id") or "n/a")
    detail_columns[1].metric("Created At", detail.get("created_at") or "n/a")
    detail_columns[2].metric("Tags", len(detail.get("tags") or []))
    detail_columns[3].metric("Seeds CSV Exists", "Yes" if _path_exists(detail.get("seeds_csv")) else "No")

    action_columns = st.columns([1.7, 1.1])
    with action_columns[0]:
        if st.button(
            "Use this benchmark preset for Run Batch",
            type="primary",
            use_container_width=True,
        ):
            _select_benchmark_preset_for_run_batch(selected_entry)
            st.rerun()
    with action_columns[1]:
        if st.button(
            "Clear selected benchmark preset",
            use_container_width=True,
            disabled=not bool(current_selected_id),
        ):
            _clear_selected_benchmark_preset()
            st.rerun()
    if not _path_exists(detail.get("seeds_csv")):
        st.warning("This benchmark preset points to a seeds CSV path that is missing on disk.")

    st.code(
        _seed_detail_text_block(
            {
                "preset_path": _display_path(detail.get("preset_path")) or "n/a",
                "seeds_csv": _display_path(detail.get("seeds_csv")) or "n/a",
                "description": detail.get("description") or "n/a",
                "tags": ", ".join(detail.get("tags") or []) or "n/a",
                "source_type": detail.get("raw_payload", {}).get("source_type") or "n/a",
                "source_curation_id": detail.get("raw_payload", {}).get("source_curation_id") or "n/a",
                "curated_seed_count": _display_summary_value(
                    detail.get("raw_payload", {}).get("curated_seed_count")
                ),
            }
        ),
        language="text",
    )
    with st.expander("Raw benchmark preset JSON"):
        st.json(detail.get("raw_payload"))


def _render_evaluation_presets_section() -> None:
    st.markdown("**Create Evaluation Preset**")
    with st.form("ui_eval_preset_form", clear_on_submit=False):
        st.text_input("Eval Preset ID", key="ui_new_eval_preset_id")
        option_columns = st.columns(4)
        with option_columns[0]:
            st.number_input("max_references", min_value=0, step=1, key="ui_new_eval_max_references")
        with option_columns[1]:
            st.number_input("max_related", min_value=0, step=1, key="ui_new_eval_max_related")
        with option_columns[2]:
            st.number_input("max_hard_negatives", min_value=0, step=1, key="ui_new_eval_max_hard_negatives")
        with option_columns[3]:
            st.number_input("top_k", min_value=1, step=1, key="ui_new_eval_top_k")
        eval_option_columns = st.columns([1.4, 1.0])
        with eval_option_columns[0]:
            st.selectbox(
                "label_source",
                options=list(SUPPORTED_LABEL_SOURCES),
                key="ui_new_eval_label_source",
            )
        with eval_option_columns[1]:
            st.checkbox("refresh", key="ui_new_eval_refresh")
        st.text_input("Description / notes", key="ui_new_eval_preset_description")
        eval_submitted = st.form_submit_button("Save Evaluation Preset", type="primary")

    if eval_submitted:
        _submit_save_evaluation_preset()

    eval_error = st.session_state.get("ui_eval_preset_error")
    if eval_error:
        st.error(eval_error)
    eval_saved_path = _optional_str(st.session_state.get("ui_eval_preset_saved_path"))
    if eval_saved_path is not None:
        st.success(f"Saved evaluation preset to `{eval_saved_path}`")

    entries, warnings = scan_evaluation_presets()
    if warnings:
        with st.expander(f"Evaluation preset warnings ({len(warnings)})"):
            for warning_message in warnings:
                st.warning(warning_message)

    if not entries:
        st.info("No evaluation presets were found yet.")
        return

    st.dataframe(
        _rows_to_table_data(build_evaluation_preset_rows(entries)),
        use_container_width=True,
        hide_index=True,
    )

    default_preset_id = choose_default_evaluation_preset_id(
        entries,
        preferred_preset_id=(
            _optional_str(st.session_state.get("ui_registry_selected_eval_preset_id"))
            or _optional_str(st.session_state.get("ui_selected_eval_preset_id"))
        ),
    )
    if default_preset_id is None:
        return
    if st.session_state.get("ui_registry_selected_eval_preset_id") not in {
        entry.preset_id for entry in entries
    }:
        st.session_state["ui_registry_selected_eval_preset_id"] = default_preset_id

    st.selectbox(
        "Evaluation preset",
        options=[entry.preset_id for entry in entries],
        key="ui_registry_selected_eval_preset_id",
    )
    selected_entry = find_evaluation_preset_entry(
        entries,
        _optional_str(st.session_state.get("ui_registry_selected_eval_preset_id")),
    )
    if selected_entry is None:
        st.info("Select an evaluation preset to inspect it.")
        return

    current_selected_id = _optional_str(st.session_state.get("ui_selected_eval_preset_id"))
    if current_selected_id is not None and not any(entry.preset_id == current_selected_id for entry in entries):
        st.warning(
            "The currently selected evaluation preset is no longer present in the registry. "
            "You can choose another one below or clear the session-state selection."
        )
    if current_selected_id == selected_entry.preset_id:
        st.success("This evaluation preset is currently selected for Run Batch convenience prefill.")

    detail = build_evaluation_preset_detail(selected_entry)
    detail_columns = st.columns(4)
    detail_columns[0].metric("Preset ID", detail.get("eval_preset_id") or "n/a")
    detail_columns[1].metric("Created At", detail.get("created_at") or "n/a")
    detail_columns[2].metric("label_source", detail.get("label_source") or "n/a")
    detail_columns[3].metric("refresh", "Yes" if detail.get("refresh") else "No")

    action_columns = st.columns([1.7, 1.1])
    with action_columns[0]:
        if st.button(
            "Use this evaluation preset for Run Batch",
            type="primary",
            use_container_width=True,
        ):
            _select_evaluation_preset_for_run_batch(selected_entry)
            st.rerun()
    with action_columns[1]:
        if st.button(
            "Clear selected evaluation preset",
            use_container_width=True,
            disabled=not bool(current_selected_id),
        ):
            _clear_selected_evaluation_preset()
            st.rerun()
    if _optional_str(detail.get("label_source")) not in SUPPORTED_LABEL_SOURCES:
        st.warning(
            "This evaluation preset uses a legacy label_source that the current Run Batch UI "
            "does not support directly. Selecting it will fall back to the current supported label_source."
        )

    st.code(
        _seed_detail_text_block(
            {
                "preset_path": _display_path(detail.get("preset_path")) or "n/a",
                "max_references": _display_summary_value(detail.get("max_references")),
                "max_related": _display_summary_value(detail.get("max_related")),
                "max_hard_negatives": _display_summary_value(detail.get("max_hard_negatives")),
                "top_k": _display_summary_value(detail.get("top_k")),
                "label_source": detail.get("label_source") or "n/a",
                "refresh": detail.get("refresh"),
                "description": detail.get("description") or "n/a",
            }
        ),
        language="text",
    )
    with st.expander("Raw evaluation preset JSON"):
        st.json(detail.get("raw_payload"))

def _render_overview(bundle: BatchUiBundle | None) -> None:
    if bundle is None:
        _render_no_loaded_batch_message()
        return

    manifest = bundle.manifest
    aggregate = bundle.aggregate_summary

    status_message = (
        f"{manifest.completed_seed_count} completed / "
        f"{manifest.failed_seed_count} failed / "
        f"{manifest.seed_count} total"
    )
    if manifest.failed_seed_count:
        st.warning(status_message)
    else:
        st.success(status_message)

    meta_columns = st.columns(4)
    meta_columns[0].metric("Batch ID", manifest.batch_id)
    meta_columns[1].metric("Ranking Metric", aggregate.ranking_metric or "n/a")
    meta_columns[2].metric("Completed Seeds", manifest.completed_seed_count)
    meta_columns[3].metric("Failed Seeds", manifest.failed_seed_count)

    left_column, right_column = st.columns([1.2, 1.0])
    with left_column:
        st.subheader("Batch Metadata")
        st.markdown(f"**Batch Directory:** `{bundle.batch_dir}`")
        st.markdown(f"**Theory Config:** `{manifest.theory_config}`")
        st.markdown(f"**Seeds CSV:** `{manifest.seeds_csv}`")
        st.markdown(f"**Created At:** `{manifest.created_at}`")
        st.markdown(f"**Completed At:** `{manifest.completed_at or 'n/a'}`")
        st.markdown(f"**Status:** `{manifest.status}`")
    with right_column:
        st.subheader("Effective Options")
        st.json(manifest.options.model_dump(mode="json"))

    st.subheader("Aggregate Metrics")
    st.dataframe(
        _rows_to_table_data(_metric_summary_rows(bundle)),
        use_container_width=True,
        hide_index=True,
    )

    with st.expander("Raw aggregate_summary.json"):
        st.json(aggregate.model_dump(mode="json"))


def _render_seed_explorer(bundle: BatchUiBundle | None) -> None:
    if bundle is None:
        _render_no_loaded_batch_message()
        return

    st.subheader("Seed Explorer")

    control_columns = st.columns([1.2, 1.5, 1.2, 1.0])
    with control_columns[0]:
        st.radio(
            "Show",
            options=["all", "completed only", "failed only"],
            horizontal=True,
            key="ui_seed_status_mode",
        )
    with control_columns[1]:
        st.text_input("DOI contains", key="ui_seed_doi_filter")
    with control_columns[2]:
        st.selectbox(
            "Sort by",
            options=_seed_sort_options(),
            format_func=lambda value: "Batch Index" if value == "batch_index" else METRIC_LABELS[value],
            key="ui_seed_sort_metric",
        )
    with control_columns[3]:
        st.checkbox("Descending", key="ui_seed_sort_desc")

    filtered_rows = _filter_seed_rows(
        bundle.seed_table_rows,
        status_mode=st.session_state["ui_seed_status_mode"],
        doi_filter=st.session_state["ui_seed_doi_filter"],
        sort_metric=st.session_state["ui_seed_sort_metric"],
        descending=bool(st.session_state["ui_seed_sort_desc"]),
    )

    st.caption(f"{len(filtered_rows)} seed row(s) shown")
    if filtered_rows:
        st.dataframe(
            _rows_to_table_data(_seed_table_display_rows(filtered_rows)),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No seeds match the current filters.")
        return

    selectable_indices = [int(row["batch_index"]) for row in filtered_rows]
    selected_index = st.session_state.get("ui_selected_seed_index")
    if selected_index not in selectable_indices:
        st.session_state["ui_selected_seed_index"] = selectable_indices[0]

    st.selectbox(
        "Selected seed",
        options=selectable_indices,
        format_func=lambda value: _seed_option_label(bundle.seed_rows_by_batch_index.get(value, {})),
        key="ui_selected_seed_index",
    )
    selected_row = bundle.seed_rows_by_batch_index[int(st.session_state["ui_selected_seed_index"])]

    st.subheader("Seed Details")
    if selected_row.get("status") == "failed":
        st.error(selected_row.get("error_message") or "This seed failed without an error message.")
    elif selected_row.get("status") == "completed":
        st.success("Seed completed successfully.")

    detail_left, detail_right = st.columns([1.1, 1.0])
    with detail_left:
        st.markdown(f"**DOI:** `{selected_row.get('doi')}`")
        st.markdown(f"**Status:** `{selected_row.get('status')}`")
        st.markdown(f"**Run Directory:** `{selected_row.get('run_dir') or 'n/a'}`")
        st.markdown(f"**Experiment ID:** `{selected_row.get('experiment_id') or 'n/a'}`")
        st.markdown(f"**Failed Stage:** `{selected_row.get('failed_stage') or 'n/a'}`")
        st.markdown(f"**Error Type:** `{selected_row.get('error_type') or 'n/a'}`")
        st.markdown(f"**Error Message:** `{selected_row.get('error_message') or 'n/a'}`")
    with detail_right:
        st.markdown("**Metrics**")
        metrics_payload = selected_row.get("metrics") or {}
        if metrics_payload:
            st.json(metrics_payload)
        else:
            st.info("No metrics recorded for this seed.")

    with st.expander("Important Artifact Paths"):
        st.json(_artifact_paths_for_seed(selected_row))


def _render_worst_cases_and_failures(bundle: BatchUiBundle | None) -> None:
    if bundle is None:
        _render_no_loaded_batch_message()
        return

    worst_cases = bundle.worst_cases

    st.subheader("Best Seeds")
    _render_ranked_seed_group(worst_cases.get("best_seeds", []), empty_message="No completed best seeds were recorded.")

    st.subheader("Worst Seeds")
    _render_ranked_seed_group(worst_cases.get("worst_seeds", []), empty_message="No completed worst seeds were recorded.")

    st.subheader("Failed Seeds")
    failed_rows = worst_cases.get("failed_seeds", [])
    if not failed_rows:
        st.success("No failed seeds were recorded in worst_cases.json.")
        return

    st.error(f"{len(failed_rows)} failed seed(s) recorded")
    st.dataframe(
        _rows_to_table_data(_failed_seed_display_rows(failed_rows)),
        use_container_width=True,
        hide_index=True,
    )
    for failed_seed in failed_rows:
        title = f"{failed_seed.get('batch_index', '?')}: {failed_seed.get('doi', 'unknown')} [{failed_seed.get('error_type', 'error')}]"
        with st.expander(title):
            st.json(failed_seed)


def _render_seed_detail(bundle: BatchUiBundle | None) -> None:
    if bundle is None:
        _render_no_loaded_batch_message()
        return

    st.subheader("Seed Detail")

    subset = str(st.session_state.get("ui_seed_detail_subset", "all"))
    subset_rows = select_seed_rows_for_subset(
        bundle.seed_table_rows,
        subset=subset,
        worst_cases=bundle.worst_cases,
    )
    if not subset_rows:
        subset_rows = bundle.seed_table_rows
        st.session_state["ui_seed_detail_subset"] = "all"

    preferred_doi = st.session_state.get("ui_seed_detail_doi")
    preferred_batch_index = st.session_state.get("ui_selected_seed_index")
    default_doi = choose_default_seed_doi(
        subset_rows,
        preferred_doi=str(preferred_doi) if preferred_doi else None,
        preferred_batch_index=int(preferred_batch_index) if preferred_batch_index is not None else None,
    )
    available_dois = [str(row.get("doi")) for row in subset_rows if row.get("doi")]
    if default_doi is not None and default_doi not in available_dois:
        available_dois.insert(0, default_doi)
    if st.session_state.get("ui_seed_detail_doi") not in available_dois:
        st.session_state["ui_seed_detail_doi"] = default_doi or ""

    control_columns = st.columns([1.3, 2.4])
    with control_columns[0]:
        st.radio(
            "Seed subset",
            options=list(SEED_DETAIL_SUBSETS),
            horizontal=True,
            key="ui_seed_detail_subset",
            format_func=_seed_detail_subset_label,
        )
    subset_rows = select_seed_rows_for_subset(
        bundle.seed_table_rows,
        subset=str(st.session_state.get("ui_seed_detail_subset", "all")),
        worst_cases=bundle.worst_cases,
    )
    if not subset_rows:
        st.info("No seeds are available for the selected Seed Detail subset.")
        return

    available_dois = [str(row.get("doi")) for row in subset_rows if row.get("doi")]
    if st.session_state.get("ui_seed_detail_doi") not in available_dois:
        st.session_state["ui_seed_detail_doi"] = choose_default_seed_doi(
            subset_rows,
            preferred_batch_index=int(preferred_batch_index) if preferred_batch_index is not None else None,
        ) or available_dois[0]

    with control_columns[1]:
        st.selectbox(
            "DOI",
            options=available_dois,
            key="ui_seed_detail_doi",
            format_func=lambda doi: _seed_detail_doi_label(subset_rows, doi),
        )
    st.caption(f"{len(subset_rows)} seed(s) available in the selected subset")

    selected_row = find_seed_row_by_doi(
        subset_rows,
        str(st.session_state.get("ui_seed_detail_doi", "")),
    )
    if selected_row is None:
        st.info("Select a seed DOI to inspect its batch artifacts.")
        return

    sections = build_seed_detail_sections(selected_row)
    status_value = str(sections.identity.get("status") or "unknown")
    if status_value == "failed":
        st.error(sections.failure.get("error_message") or "This seed failed during batch execution.")
    elif status_value == "completed":
        st.success("Seed completed successfully.")
    else:
        st.info(f"Seed status: {status_value}")

    identity_columns = st.columns(3)
    identity_columns[0].metric("Batch Index", sections.identity.get("batch_index") or "n/a")
    identity_columns[1].metric("Status", sections.identity.get("status") or "n/a")
    identity_columns[2].metric("Reused Existing Run", _yes_no_value(sections.identity.get("reused_existing_run")))

    identity_left, identity_right = st.columns([1.2, 1.0])
    with identity_left:
        st.markdown(f"**DOI:** `{sections.identity.get('doi') or 'n/a'}`")
        st.markdown(f"**Run Directory:** `{_display_path(sections.identity.get('run_dir')) or 'n/a'}`")
        st.markdown(f"**Experiment ID:** `{sections.identity.get('experiment_id') or 'n/a'}`")
    with identity_right:
        optional_fields = {
            "Seed OpenAlex ID": selected_row.get("seed_openalex_id"),
            "Candidate Count": selected_row.get("candidate_count"),
            "Judged Count": selected_row.get("judged_count"),
        }
        for label, value in optional_fields.items():
            if value not in (None, ""):
                st.markdown(f"**{label}:** `{value}`")

    st.subheader("Metrics")
    if sections.metrics:
        metric_columns = st.columns(min(3, len(sections.metrics)))
        for index, (metric_name, metric_value) in enumerate(sections.metrics.items()):
            metric_columns[index % len(metric_columns)].metric(_metric_label(metric_name), metric_value)
    else:
        st.info("No numeric metrics were recorded for this seed.")

    if sections.extra_metrics:
        with st.expander("Additional Metrics"):
            st.json(sections.extra_metrics)

    st.subheader("Failure Information")
    if sections.failure:
        st.code(_seed_detail_text_block(sections.failure), language="text")
    else:
        st.success("No failure information recorded for this seed.")

    st.subheader("Artifact Paths")
    if sections.artifact_paths:
        st.code(
            _seed_detail_text_block(
                {
                    field_name: _display_path(path_value)
                    for field_name, path_value in sections.artifact_paths.items()
                }
            ),
            language="text",
        )
    else:
        st.info("No artifact paths were recorded for this seed.")


def _render_diagnostics(bundle: BatchUiBundle | None) -> None:
    if bundle is None:
        _render_no_loaded_batch_message()
        return

    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ModuleNotFoundError:
        st.error("Plotly is not installed. Install project dependencies to use the Diagnostics tab.")
        return

    st.subheader("Diagnostics")

    all_numeric_metrics = available_numeric_metrics(bundle.seed_table_rows)
    if not all_numeric_metrics:
        st.info("No numeric per-seed metrics are available in the loaded batch artifacts.")
        return

    _sync_diagnostics_metric_state(all_numeric_metrics)

    control_columns = st.columns([1.2, 1.2, 1.2, 1.2, 1.0])
    with control_columns[0]:
        st.selectbox(
            "Status filter",
            options=["completed only", "all"],
            key="ui_diag_status_mode",
        )

    filtered_rows = filter_diagnostic_rows(
        bundle.seed_table_rows,
        status_mode=str(st.session_state.get("ui_diag_status_mode", "completed only")),
    )
    filtered_metrics = available_numeric_metrics(filtered_rows)
    if not filtered_metrics:
        st.info("No numeric per-seed metrics are available for the selected diagnostics filter.")
        return

    _sync_diagnostics_metric_state(filtered_metrics)
    max_limit = max(1, min(10, len(filtered_rows)))
    current_limit = int(st.session_state.get("ui_diag_limit", 5))
    if current_limit < 1 or current_limit > max_limit:
        st.session_state["ui_diag_limit"] = min(max(1, current_limit), max_limit)

    with control_columns[1]:
        st.selectbox(
            "Primary metric",
            options=filtered_metrics,
            format_func=_metric_label,
            key="ui_diag_primary_metric",
        )
    with control_columns[2]:
        st.selectbox(
            "Scatter X metric",
            options=filtered_metrics,
            format_func=_metric_label,
            key="ui_diag_x_metric",
        )
    with control_columns[3]:
        st.selectbox(
            "Scatter Y metric",
            options=filtered_metrics,
            format_func=_metric_label,
            key="ui_diag_y_metric",
        )
    with control_columns[4]:
        st.slider(
            "Top/Bottom N",
            min_value=1,
            max_value=max_limit,
            step=1,
            key="ui_diag_limit",
        )

    primary_metric = str(st.session_state.get("ui_diag_primary_metric", ""))
    x_metric = str(st.session_state.get("ui_diag_x_metric", ""))
    y_metric = str(st.session_state.get("ui_diag_y_metric", ""))
    ranking_metric = choose_ranking_metric(bundle.aggregate_summary.ranking_metric, filtered_metrics)
    completed_seed_count = sum(1 for row in filtered_rows if row.get("status") == "completed")

    st.caption(
        "Loaded batch diagnostics are based only on the current in-memory batch bundle. "
        "No evaluation logic is re-run here."
    )
    summary_columns = st.columns(4)
    summary_columns[0].metric("Batch ID", bundle.manifest.batch_id)
    summary_columns[1].metric("Ranking Metric", _metric_label(ranking_metric) if ranking_metric else "n/a")
    summary_columns[2].metric("Completed Seeds in View", completed_seed_count)
    summary_columns[3].metric("Available Metrics", len(filtered_metrics))
    st.markdown(
        f"**Metrics Available:** {', '.join(_metric_label(metric_name) for metric_name in filtered_metrics)}"
    )

    histogram_values = metric_values(filtered_rows, primary_metric)
    distribution_left, distribution_right = st.columns(2)
    with distribution_left:
        if histogram_values:
            histogram_figure = go.Figure(
                data=[go.Histogram(x=histogram_values, marker_color="#1f77b4")]
            )
            histogram_figure.update_layout(
                title=f"Distribution of {_metric_label(primary_metric)} (n={len(histogram_values)})",
                xaxis_title=_metric_label(primary_metric),
                yaxis_title="Seed Count",
            )
            st.plotly_chart(histogram_figure, use_container_width=True)
        else:
            st.info(f"No valid values are available for {_metric_label(primary_metric)}.")
    with distribution_right:
        if histogram_values:
            box_figure = go.Figure(
                data=[go.Box(y=histogram_values, boxmean=True, marker_color="#457b9d")]
            )
            box_figure.update_layout(
                title=f"Spread of {_metric_label(primary_metric)}",
                yaxis_title=_metric_label(primary_metric),
            )
            st.plotly_chart(box_figure, use_container_width=True)
        else:
            st.info(f"No box plot is available for {_metric_label(primary_metric)}.")

    st.subheader("Metric Scatter")
    if x_metric == y_metric:
        st.info("Choose two different metrics to render the scatter plot.")
    else:
        scatter_rows = scatter_points(filtered_rows, x_metric=x_metric, y_metric=y_metric)
        if len(scatter_rows) < 2:
            st.info("At least two seeds with both selected metrics are needed for the scatter plot.")
        else:
            scatter_figure = go.Figure(
                data=[
                    go.Scatter(
                        x=[row[x_metric] for row in scatter_rows],
                        y=[row[y_metric] for row in scatter_rows],
                        mode="markers",
                        marker={"size": 10, "color": "#2a9d8f"},
                        customdata=[
                            [
                                row.get("doi") or "n/a",
                                row.get("status") or "n/a",
                                _display_path(row.get("run_dir")) or "n/a",
                                row.get("experiment_id") or "n/a",
                            ]
                            for row in scatter_rows
                        ],
                        hovertemplate=(
                            "DOI: %{customdata[0]}<br>"
                            "Status: %{customdata[1]}<br>"
                            "Run Dir: %{customdata[2]}<br>"
                            "Experiment ID: %{customdata[3]}<br>"
                            f"{_metric_label(x_metric)}: %{{x}}<br>"
                            f"{_metric_label(y_metric)}: %{{y}}<extra></extra>"
                        ),
                    )
                ]
            )
            scatter_figure.update_layout(
                title=f"{_metric_label(x_metric)} vs {_metric_label(y_metric)}",
                xaxis_title=_metric_label(x_metric),
                yaxis_title=_metric_label(y_metric),
            )
            st.plotly_chart(scatter_figure, use_container_width=True)

    st.subheader("Best and Worst Seeds")
    if ranking_metric is None:
        st.info("No numeric ranking metric is available for best/worst seed diagnostics.")
        return

    limit = int(st.session_state.get("ui_diag_limit", 5))
    best_rows, worst_rows = best_and_worst_rows(
        filtered_rows,
        ranking_metric=ranking_metric,
        limit=limit,
    )
    if not best_rows and not worst_rows:
        st.info("No completed seeds with a ranking metric are available for the bar chart.")
        return

    best_worst_figure = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=(
            f"Top {len(best_rows)} by {_metric_label(ranking_metric)}",
            f"Bottom {len(worst_rows)} by {_metric_label(ranking_metric)}",
        ),
        horizontal_spacing=0.18,
    )
    if best_rows:
        best_worst_figure.add_trace(
            go.Bar(
                x=[row[ranking_metric] for row in best_rows],
                y=[_diagnostic_seed_label(row) for row in best_rows],
                orientation="h",
                marker_color="#2a9d8f",
                customdata=[[row.get("doi") or "n/a"] for row in best_rows],
                hovertemplate=(
                    "DOI: %{customdata[0]}<br>"
                    f"{_metric_label(ranking_metric)}: %{{x}}<extra></extra>"
                ),
            ),
            row=1,
            col=1,
        )
        best_worst_figure.update_yaxes(autorange="reversed", row=1, col=1)
    if worst_rows:
        best_worst_figure.add_trace(
            go.Bar(
                x=[row[ranking_metric] for row in worst_rows],
                y=[_diagnostic_seed_label(row) for row in worst_rows],
                orientation="h",
                marker_color="#e76f51",
                customdata=[[row.get("doi") or "n/a"] for row in worst_rows],
                hovertemplate=(
                    "DOI: %{customdata[0]}<br>"
                    f"{_metric_label(ranking_metric)}: %{{x}}<extra></extra>"
                ),
            ),
            row=1,
            col=2,
        )
        best_worst_figure.update_yaxes(autorange="reversed", row=1, col=2)
    best_worst_figure.update_layout(
        showlegend=False,
        height=480,
        xaxis_title=_metric_label(ranking_metric),
        xaxis2_title=_metric_label(ranking_metric),
    )
    st.plotly_chart(best_worst_figure, use_container_width=True)


def _render_comparison(primary_bundle: BatchUiBundle | None) -> None:
    st.subheader("Comparison")
    st.caption(
        "Use the currently loaded batch as the primary/reference batch and load a second batch "
        "directory for read-only paired comparison on overlapping seeds."
    )

    if primary_bundle is None:
        _render_no_loaded_batch_message()
        return

    load_columns = st.columns([4.0, 1.2])
    with load_columns[0]:
        st.text_input(
            "Secondary batch directory",
            placeholder="runs/batches/batch_006",
            key="ui_comparison_secondary_dir_input",
        )
    with load_columns[1]:
        st.write("")
        if st.button("Load Secondary Batch", use_container_width=True):
            _load_secondary_comparison_batch()

    comparison_error = st.session_state.get("ui_comparison_error")
    if comparison_error:
        st.error(comparison_error)

    secondary_bundle = st.session_state.get("ui_comparison_secondary_bundle")
    if secondary_bundle is None:
        st.info("Load a secondary batch directory to compare it with the current primary batch.")
        return

    aligned_rows = align_common_seed_rows(
        primary_bundle.seed_table_rows,
        secondary_bundle.seed_table_rows,
    )
    primary_seed_count = len(primary_bundle.seed_table_rows)
    secondary_seed_count = len(secondary_bundle.seed_table_rows)
    common_doi_count = len(aligned_rows)
    common_completed_count = common_completed_seed_count(aligned_rows)

    count_columns = st.columns(4)
    count_columns[0].metric("Primary Seeds", primary_seed_count)
    count_columns[1].metric("Secondary Seeds", secondary_seed_count)
    count_columns[2].metric("Common DOIs", common_doi_count)
    count_columns[3].metric("Common Completed", common_completed_count)

    st.markdown(f"**Primary Batch:** `{primary_bundle.manifest.batch_id}`")
    st.markdown(f"**Secondary Batch:** `{secondary_bundle.manifest.batch_id}`")
    st.markdown(f"**Secondary Batch Dir:** `{_display_path(secondary_bundle.batch_dir)}`")

    compatibility_warning_list = compatibility_warnings(primary_bundle.manifest, secondary_bundle.manifest)
    for warning_message in compatibility_warning_list:
        st.warning(warning_message)

    if not aligned_rows:
        st.warning("The selected primary and secondary batches do not share any DOI overlap.")
        return

    common_metrics = common_numeric_metrics(aligned_rows)
    if not common_metrics:
        st.info("The overlapping seeds do not share any numeric metrics that can be compared.")
        return

    overlap_counts = metric_overlap_counts(aligned_rows)
    if overlap_counts:
        st.markdown(
            "**Common Completed Per Metric:** "
            + ", ".join(
                f"{_metric_label(metric_name)}={overlap_counts[metric_name]}"
                for metric_name in overlap_counts
            )
        )

    _sync_comparison_metric_state(primary_bundle.aggregate_summary.ranking_metric, common_metrics)
    selected_metric = str(st.session_state.get("ui_comparison_metric", ""))
    completed_metric_rows = paired_metric_rows(
        aligned_rows,
        metric_name=selected_metric,
        status_mode=COMPARISON_STATUS_OPTIONS[0],
    )
    max_limit = max(1, min(10, len(completed_metric_rows) if completed_metric_rows else common_doi_count))
    current_limit = int(st.session_state.get("ui_comparison_limit", 5))
    if current_limit < 1 or current_limit > max_limit:
        st.session_state["ui_comparison_limit"] = min(max(1, current_limit), max_limit)

    control_columns = st.columns([1.5, 1.7, 1.0, 1.3])
    with control_columns[0]:
        st.selectbox(
            "Comparison metric",
            options=common_metrics,
            format_func=_metric_label,
            key="ui_comparison_metric",
        )
    with control_columns[1]:
        st.selectbox(
            "Status subset",
            options=list(COMPARISON_STATUS_OPTIONS),
            key="ui_comparison_status_mode",
        )
    with control_columns[2]:
        st.slider(
            "Top/Bottom N",
            min_value=1,
            max_value=max_limit,
            step=1,
            key="ui_comparison_limit",
        )
    with control_columns[3]:
        st.text_input("Table DOI filter", key="ui_comparison_doi_filter")

    selected_metric = str(st.session_state.get("ui_comparison_metric", ""))
    status_mode = str(st.session_state.get("ui_comparison_status_mode", COMPARISON_STATUS_OPTIONS[0]))
    paired_rows = paired_metric_rows(
        aligned_rows,
        metric_name=selected_metric,
        status_mode=status_mode,
    )
    if not paired_rows:
        st.info("No common seeds with the selected metric are available for the current comparison filter.")
        return

    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ModuleNotFoundError:
        st.error("Plotly is not installed. Install project dependencies to use the Comparison tab.")
        return

    summary = comparison_metric_summary(paired_rows)
    guardrail_completed_rows = paired_metric_rows(
        aligned_rows,
        metric_name=selected_metric,
        status_mode=COMPARISON_STATUS_OPTIONS[0],
    )
    comparison_guardrail = evaluate_decision_guardrails(
        selected_metric=selected_metric,
        common_doi_count=common_doi_count,
        common_completed_seed_count=common_completed_count,
        summary=(comparison_metric_summary(guardrail_completed_rows) if guardrail_completed_rows else None),
        paired_seed_count=len(guardrail_completed_rows),
    )
    summary_columns = st.columns(4)
    summary_columns[0].metric("Metric", _metric_label(selected_metric))
    summary_columns[1].metric("Paired Seeds in View", len(paired_rows))
    summary_columns[2].metric("Primary Mean", summary.primary_mean if summary.primary_mean is not None else "n/a")
    summary_columns[3].metric(
        "Secondary Mean",
        summary.secondary_mean if summary.secondary_mean is not None else "n/a",
    )

    summary_left, summary_right = st.columns([1.3, 1.0])
    with summary_left:
        st.markdown(f"**Primary Theory Config:** `{_display_path(primary_bundle.manifest.theory_config)}`")
        st.markdown(f"**Secondary Theory Config:** `{_display_path(secondary_bundle.manifest.theory_config)}`")
        st.markdown(
            f"**Primary Mean / Median:** `{summary.primary_mean}` / `{summary.primary_median}`"
        )
        st.markdown(
            f"**Secondary Mean / Median:** `{summary.secondary_mean}` / `{summary.secondary_median}`"
        )
    with summary_right:
        st.markdown(
            f"**Raw Delta Mean / Median:** `{summary.raw_delta_mean}` / `{summary.raw_delta_median}`"
        )
        st.markdown(
            f"**Improvement Delta Mean / Median:** "
            f"`{summary.improvement_delta_mean}` / `{summary.improvement_delta_median}`"
        )
        st.markdown(
            f"**Wins / Losses / Ties:** `{summary.wins}` / `{summary.losses}` / `{summary.ties}`"
        )

    _render_guardrail_panel(comparison_guardrail, title="Evidence Guardrails")

    st.subheader("Primary vs Secondary Parity")
    if len(paired_rows) < 2:
        st.info("At least two paired seeds are needed to render the parity scatter plot.")
    else:
        parity_x = [row["primary_metric_value"] for row in paired_rows]
        parity_y = [row["secondary_metric_value"] for row in paired_rows]
        parity_figure = go.Figure()
        parity_figure.add_trace(
            go.Scatter(
                x=parity_x,
                y=parity_y,
                mode="markers",
                marker={"size": 10, "color": "#2a9d8f"},
                customdata=[
                    [
                        row.get("doi") or "n/a",
                        row.get("primary_status") or "n/a",
                        row.get("secondary_status") or "n/a",
                    ]
                    for row in paired_rows
                ],
                hovertemplate=(
                    "DOI: %{customdata[0]}<br>"
                    "Primary Status: %{customdata[1]}<br>"
                    "Secondary Status: %{customdata[2]}<br>"
                    f"Primary {_metric_label(selected_metric)}: %{{x}}<br>"
                    f"Secondary {_metric_label(selected_metric)}: %{{y}}<extra></extra>"
                ),
            )
        )
        min_value = min(parity_x + parity_y)
        max_value = max(parity_x + parity_y)
        if min_value != max_value:
            parity_figure.add_trace(
                go.Scatter(
                    x=[min_value, max_value],
                    y=[min_value, max_value],
                    mode="lines",
                    line={"dash": "dash", "color": "#6c757d"},
                    hoverinfo="skip",
                    showlegend=False,
                )
            )
        parity_figure.update_layout(
            title=f"Primary vs Secondary {_metric_label(selected_metric)}",
            xaxis_title=f"Primary {_metric_label(selected_metric)}",
            yaxis_title=f"Secondary {_metric_label(selected_metric)}",
        )
        st.plotly_chart(parity_figure, use_container_width=True)

    st.subheader("Delta Distribution")
    delta_figure = go.Figure(
        data=[
            go.Histogram(
                x=[row["improvement_delta"] for row in paired_rows],
                marker_color="#457b9d",
            )
        ]
    )
    delta_figure.update_layout(
        title=f"Improvement Delta Distribution for {_metric_label(selected_metric)}",
        xaxis_title="Improvement Delta",
        yaxis_title="Seed Count",
    )
    st.plotly_chart(delta_figure, use_container_width=True)

    st.subheader("Per-Seed Improvements and Regressions")
    completed_metric_rows = paired_metric_rows(
        aligned_rows,
        metric_name=selected_metric,
        status_mode=COMPARISON_STATUS_OPTIONS[0],
    )
    if not completed_metric_rows:
        st.info("No common completed seeds with the selected metric are available for the delta bar chart.")
    else:
        limit = int(st.session_state.get("ui_comparison_limit", 5))
        top_improvements = sorted(
            completed_metric_rows,
            key=lambda row: (row["improvement_delta"], str(row.get("doi", ""))),
            reverse=True,
        )[:limit]
        top_regressions = sorted(
            completed_metric_rows,
            key=lambda row: (row["improvement_delta"], str(row.get("doi", ""))),
        )[:limit]
        delta_bar_figure = make_subplots(
            rows=1,
            cols=2,
            subplot_titles=(
                f"Top {len(top_improvements)} Improvements",
                f"Top {len(top_regressions)} Regressions",
            ),
            horizontal_spacing=0.18,
        )
        if top_improvements:
            delta_bar_figure.add_trace(
                go.Bar(
                    x=[row["improvement_delta"] for row in top_improvements],
                    y=[row.get("doi") or "n/a" for row in top_improvements],
                    orientation="h",
                    marker_color="#2a9d8f",
                    hovertemplate=(
                        "DOI: %{y}<br>"
                        "Improvement Delta: %{x}<extra></extra>"
                    ),
                ),
                row=1,
                col=1,
            )
            delta_bar_figure.update_yaxes(autorange="reversed", row=1, col=1)
        if top_regressions:
            delta_bar_figure.add_trace(
                go.Bar(
                    x=[row["improvement_delta"] for row in top_regressions],
                    y=[row.get("doi") or "n/a" for row in top_regressions],
                    orientation="h",
                    marker_color="#e76f51",
                    hovertemplate=(
                        "DOI: %{y}<br>"
                        "Improvement Delta: %{x}<extra></extra>"
                    ),
                ),
                row=1,
                col=2,
            )
            delta_bar_figure.update_yaxes(autorange="reversed", row=1, col=2)
        delta_bar_figure.update_layout(
            showlegend=False,
            height=500,
            xaxis_title="Improvement Delta",
            xaxis2_title="Improvement Delta",
        )
        st.plotly_chart(delta_bar_figure, use_container_width=True)

    st.subheader("Comparison Table")
    doi_filter = str(st.session_state.get("ui_comparison_doi_filter", "")).strip().lower()
    table_rows = paired_rows
    if doi_filter:
        table_rows = [
            row for row in table_rows
            if doi_filter in str(row.get("doi", "")).lower()
        ]

    if not table_rows:
        st.info("No comparison rows match the current table filter.")
    else:
        st.dataframe(
            _rows_to_table_data(
                [
                    {
                        "doi": row.get("doi"),
                        "primary_status": row.get("primary_status"),
                        "secondary_status": row.get("secondary_status"),
                        "primary_metric_value": row.get("primary_metric_value"),
                        "secondary_metric_value": row.get("secondary_metric_value"),
                        "raw_delta": row.get("raw_delta"),
                        "improvement_delta": row.get("improvement_delta"),
                        "primary_run_dir": _display_path(row.get("primary_run_dir")),
                        "secondary_run_dir": _display_path(row.get("secondary_run_dir")),
                        "primary_experiment_id": row.get("primary_experiment_id"),
                        "secondary_experiment_id": row.get("secondary_experiment_id"),
                    }
                    for row in table_rows
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )

    study_source_assessment = _get_current_study_source_assessment(primary_bundle, secondary_bundle)
    _render_study_source_context_panel(study_source_assessment)

    st.subheader("Decision / Save Comparison")
    context_columns = st.columns(5)
    context_columns[0].metric("Primary Batch", primary_bundle.manifest.batch_id)
    context_columns[1].metric("Secondary Batch", secondary_bundle.manifest.batch_id)
    context_columns[2].metric("Metric", _metric_label(selected_metric))
    context_columns[3].metric("Common DOIs", common_doi_count)
    context_columns[4].metric("Common Completed", common_completed_count)

    with st.form("ui_comparison_save_form", clear_on_submit=False):
        st.text_input("Comparison ID", key="ui_comparison_save_id")
        decision_columns = st.columns([1.3, 1.0])
        with decision_columns[0]:
            st.text_input("Reviewer", key="ui_comparison_reviewer")
        with decision_columns[1]:
            st.selectbox(
                "Decision status",
                options=list(DECISION_STATUS_OPTIONS),
                key="ui_comparison_decision_status",
            )
        st.text_area("Notes / rationale", key="ui_comparison_notes", height=120)
        save_submitted = st.form_submit_button("Save Comparison", type="primary")

    if save_submitted:
        _submit_save_comparison(
            primary_bundle=primary_bundle,
            secondary_bundle=secondary_bundle,
            selected_metric=selected_metric,
            status_mode=status_mode,
            common_doi_count=common_doi_count,
            common_completed_seed_count=common_completed_count,
            compatibility_warning_list=compatibility_warning_list,
            paired_rows=paired_rows,
            summary=summary,
        )

    save_error = st.session_state.get("ui_comparison_save_error")
    if save_error:
        st.error(save_error)

    save_result = st.session_state.get("ui_comparison_save_result")
    if isinstance(save_result, ComparisonSaveResult):
        st.success(f"Saved comparison to {_display_path(save_result.comparison_dir)}")
        with st.expander("Saved artifact paths"):
            st.json(
                {
                    "comparison_manifest.json": _display_path(save_result.manifest_path),
                    "paired_seed_table.jsonl": _display_path(save_result.paired_seed_table_path),
                    "decision_record.json": _display_path(save_result.decision_record_path),
                }
            )

    if isinstance(save_result, ComparisonSaveResult) and not st.session_state.get("ui_review_packet_comparison_id"):
        st.session_state["ui_review_packet_comparison_id"] = save_result.comparison_id

    _sync_review_packet_metric_state(selected_metric, common_metrics)
    packet_metric = str(st.session_state.get("ui_review_packet_metric", ""))
    packet_focus_rows = paired_metric_rows(
        aligned_rows,
        metric_name=packet_metric,
        status_mode=COMPARISON_STATUS_OPTIONS[0],
    )
    packet_summary = comparison_metric_summary(packet_focus_rows) if packet_focus_rows else None

    st.subheader("Export Review Packet")
    packet_context_columns = st.columns(5)
    packet_context_columns[0].metric("Primary Batch", primary_bundle.manifest.batch_id)
    packet_context_columns[1].metric("Secondary Batch", secondary_bundle.manifest.batch_id)
    packet_context_columns[2].metric("Packet Metric", _metric_label(packet_metric))
    packet_context_columns[3].metric("Common DOIs", common_doi_count)
    packet_context_columns[4].metric("Common Completed", common_completed_count)
    st.markdown(f"**Primary Theory Config:** `{_display_path(primary_bundle.manifest.theory_config)}`")
    st.markdown(f"**Secondary Theory Config:** `{_display_path(secondary_bundle.manifest.theory_config)}`")
    if not packet_focus_rows:
        st.info("No common completed seeds are available for the selected packet metric yet.")

    with st.form("ui_review_packet_export_form", clear_on_submit=False):
        packet_identity_columns = st.columns([1.0, 1.2, 1.0])
        with packet_identity_columns[0]:
            st.text_input("Packet ID", key="ui_review_packet_id")
        with packet_identity_columns[1]:
            st.text_input("Comparison ID", key="ui_review_packet_comparison_id")
        with packet_identity_columns[2]:
            st.text_input("Reviewer", key="ui_review_packet_reviewer")

        packet_option_columns = st.columns([1.4, 1.0, 1.0])
        with packet_option_columns[0]:
            st.selectbox(
                "Selected metric for packet focus",
                options=common_metrics,
                format_func=_metric_label,
                key="ui_review_packet_metric",
            )
        with packet_option_columns[1]:
            st.number_input(
                "Max regressions",
                min_value=1,
                step=1,
                key="ui_review_packet_max_regressions",
            )
        with packet_option_columns[2]:
            st.number_input(
                "Max improvements",
                min_value=1,
                step=1,
                key="ui_review_packet_max_improvements",
            )
        packet_submitted = st.form_submit_button("Export Review Packet", type="primary")

    if packet_submitted:
        packet_metric = str(st.session_state.get("ui_review_packet_metric", ""))
        packet_focus_rows = paired_metric_rows(
            aligned_rows,
            metric_name=packet_metric,
            status_mode=COMPARISON_STATUS_OPTIONS[0],
        )
        packet_summary = comparison_metric_summary(packet_focus_rows) if packet_focus_rows else None
        _submit_review_packet_export(
            primary_bundle=primary_bundle,
            secondary_bundle=secondary_bundle,
            compatibility_warning_list=compatibility_warning_list,
            common_doi_count=common_doi_count,
            common_completed_seed_count=common_completed_count,
            paired_rows=packet_focus_rows,
            summary=packet_summary,
        )

    review_packet_error = st.session_state.get("ui_review_packet_error")
    if review_packet_error:
        st.error(review_packet_error)

    review_packet_result = st.session_state.get("ui_review_packet_result")
    if isinstance(review_packet_result, ReviewPacketExportResult):
        st.success(f"Exported review packet to {_display_path(review_packet_result.packet_dir)}")
        with st.expander("Review packet artifact paths"):
            st.json(
                {
                    "review_packet_manifest.json": _display_path(review_packet_result.manifest_path),
                    "evidence_summary.json": _display_path(review_packet_result.evidence_summary_path),
                    "top_regressions.jsonl": _display_path(review_packet_result.regressions_path),
                    "top_improvements.jsonl": _display_path(review_packet_result.improvements_path),
                    "allowed_revision_paths.json": _display_path(review_packet_result.allowed_revision_paths_path),
                    "baseline_theory_snapshot.yaml": _display_path(review_packet_result.baseline_snapshot_path),
                    "candidate_reply_TEMPLATE.yaml": _display_path(review_packet_result.candidate_template_path),
                }
            )

    if not st.session_state.get("ui_reeval_candidate_run_dir"):
        latest_candidate_result = st.session_state.get("ui_candidate_apply_result")
        if isinstance(latest_candidate_result, CandidateApplyRunResult):
            st.session_state["ui_reeval_candidate_run_dir"] = _display_path(latest_candidate_result.candidate_dir)

    _sync_reeval_metric_state(selected_metric, common_metrics)
    reeval_metric = str(st.session_state.get("ui_reeval_metric", ""))
    reeval_paired_rows = paired_metric_rows(
        aligned_rows,
        metric_name=reeval_metric,
        status_mode=status_mode,
    ) if reeval_metric else []
    reeval_summary = comparison_metric_summary(reeval_paired_rows) if reeval_paired_rows else None
    reeval_guardrail = evaluate_decision_guardrails(
        selected_metric=reeval_metric,
        common_doi_count=common_doi_count,
        common_completed_seed_count=common_completed_count,
        summary=reeval_summary,
        paired_seed_count=len(reeval_paired_rows),
    )
    candidate_run_dir_value = str(st.session_state.get("ui_reeval_candidate_run_dir", ""))
    candidate_run_context, candidate_run_load_error = _load_candidate_run_context_for_display(candidate_run_dir_value)
    candidate_run_compatibility = (
        candidate_run_compatibility_errors(
            candidate_run=candidate_run_context,
            primary_bundle=primary_bundle,
            secondary_bundle=secondary_bundle,
        )
        if candidate_run_context is not None
        else []
    )

    st.subheader("Save Re-eval Outcome")
    st.caption(
        "Loading a candidate run or using the inbox only prepares this form. "
        "Nothing is written until you click `Save Re-eval Outcome`."
    )
    outcome_context_columns = st.columns(7)
    outcome_context_columns[0].metric("Primary Batch", primary_bundle.manifest.batch_id)
    outcome_context_columns[1].metric("Secondary Batch", secondary_bundle.manifest.batch_id)
    outcome_context_columns[2].metric("Candidate ID", candidate_run_context.candidate_id if candidate_run_context is not None else "n/a")
    outcome_context_columns[3].metric("Packet ID", candidate_run_context.packet_id if candidate_run_context is not None else "n/a")
    outcome_context_columns[4].metric("Comparison ID", candidate_run_context.comparison_id if candidate_run_context is not None else "n/a")
    outcome_context_columns[5].metric("Common DOIs", common_doi_count)
    outcome_context_columns[6].metric("Common Completed", common_completed_count)
    if reeval_summary is not None:
        summary_cols = st.columns(4)
        summary_cols[0].metric("Decision Metric", _metric_label(reeval_metric))
        summary_cols[1].metric("Wins", reeval_summary.wins)
        summary_cols[2].metric("Losses", reeval_summary.losses)
        summary_cols[3].metric("Ties", reeval_summary.ties)
    _render_guardrail_panel(reeval_guardrail, title="Outcome Guardrails")

    if candidate_run_load_error:
        st.warning(candidate_run_load_error)
    for message in candidate_run_compatibility:
        st.warning(message)
    if reeval_metric and not reeval_paired_rows:
        st.info("No paired comparison rows are available for the selected re-eval decision metric.")

    with st.form("ui_reeval_outcome_form", clear_on_submit=False):
        st.text_input(
            "Candidate Run Directory",
            placeholder="runs/comparisons/comparison_001/review_packets/packet_001/candidate_runs/candidate_001",
            key="ui_reeval_candidate_run_dir",
        )
        outcome_identity_columns = st.columns([1.1, 1.0, 1.1])
        with outcome_identity_columns[0]:
            st.text_input("Outcome ID", key="ui_reeval_outcome_id")
        with outcome_identity_columns[1]:
            st.text_input("Reviewer", key="ui_reeval_reviewer")
        with outcome_identity_columns[2]:
            st.selectbox(
                "Decision status",
                options=list(DECISION_STATUS_OPTIONS),
                key="ui_reeval_decision_status",
            )
        outcome_metric_columns = st.columns([1.4, 1.0])
        with outcome_metric_columns[0]:
            st.selectbox(
                "Selected metric for final decision",
                options=common_metrics,
                format_func=_metric_label,
                key="ui_reeval_metric",
            )
        with outcome_metric_columns[1]:
            st.write("")
            st.write("")
            outcome_submitted = st.form_submit_button(
                "Save Re-eval Outcome",
                type="primary",
                disabled=(
                    not bool(common_doi_count)
                    or not bool(reeval_metric)
                    or not bool(str(st.session_state.get("ui_reeval_candidate_run_dir", "")).strip())
                ),
            )
        st.text_area("Notes / rationale", key="ui_reeval_notes", height=120)

    if outcome_submitted:
        _submit_save_reeval_outcome(
            primary_bundle=primary_bundle,
            secondary_bundle=secondary_bundle,
            common_doi_count=common_doi_count,
            common_completed_seed_count=common_completed_count,
            paired_rows=reeval_paired_rows,
            summary=reeval_summary,
            guardrail_assessment=reeval_guardrail,
        )

    reeval_error = st.session_state.get("ui_reeval_error")
    if reeval_error:
        st.error(reeval_error)

    reeval_result = st.session_state.get("ui_reeval_result")
    if isinstance(reeval_result, ReevalOutcomeSaveResult):
        st.success(f"Saved re-eval outcome to {_display_path(reeval_result.outcome_dir)}")
        with st.expander("Re-eval outcome artifact paths"):
            st.json(
                {
                    "reeval_outcome_manifest.json": _display_path(reeval_result.manifest_path),
                    "reeval_paired_seed_table.jsonl": _display_path(reeval_result.paired_seed_table_path),
                    "reeval_decision_record.json": _display_path(reeval_result.decision_record_path),
                }
            )

    if not st.session_state.get("ui_promotion_candidate_run_dir"):
        latest_candidate_result = st.session_state.get("ui_candidate_apply_result")
        if isinstance(latest_candidate_result, CandidateApplyRunResult):
            st.session_state["ui_promotion_candidate_run_dir"] = _display_path(latest_candidate_result.candidate_dir)
    if not st.session_state.get("ui_promotion_outcome_dir"):
        latest_outcome_result = st.session_state.get("ui_reeval_result")
        if isinstance(latest_outcome_result, ReevalOutcomeSaveResult):
            st.session_state["ui_promotion_outcome_dir"] = _display_path(latest_outcome_result.outcome_dir)

    promotion_candidate_run_dir_value = str(st.session_state.get("ui_promotion_candidate_run_dir", ""))
    promotion_outcome_dir_value = str(st.session_state.get("ui_promotion_outcome_dir", ""))
    promotion_candidate_context, promotion_candidate_load_error = _load_candidate_run_context_for_display(
        promotion_candidate_run_dir_value
    )
    promotion_outcome_context, promotion_outcome_load_error = _load_outcome_context_for_display(
        promotion_outcome_dir_value
    )
    promotion_compatibility = (
        promotion_compatibility_errors(
            candidate_run=promotion_candidate_context,
            outcome=promotion_outcome_context,
            primary_bundle=primary_bundle,
            secondary_bundle=secondary_bundle,
        )
        if promotion_candidate_context is not None and promotion_outcome_context is not None
        else []
    )
    promotion_guardrail = _guardrail_assessment_from_outcome_context(promotion_outcome_context)
    promotion_requires_override = (
        promotion_guardrail is not None and requires_explicit_promotion_override(promotion_guardrail)
    )

    st.subheader("Promote Accepted Baseline")
    st.caption(
        "Loading an outcome or using the inbox only prepares this form. "
        "Promotion remains an explicit save step."
    )
    promotion_context_columns = st.columns(8)
    promotion_context_columns[0].metric("Candidate ID", (promotion_outcome_context.candidate_id if promotion_outcome_context is not None else None) or (promotion_candidate_context.candidate_id if promotion_candidate_context is not None else "n/a"))
    promotion_context_columns[1].metric("Packet ID", (promotion_outcome_context.packet_id if promotion_outcome_context is not None else None) or (promotion_candidate_context.packet_id if promotion_candidate_context is not None else "n/a"))
    promotion_context_columns[2].metric("Comparison ID", (promotion_outcome_context.comparison_id if promotion_outcome_context is not None else None) or (promotion_candidate_context.comparison_id if promotion_candidate_context is not None else "n/a"))
    promotion_context_columns[3].metric(
        "Primary Batch",
        (
            promotion_outcome_context.manifest.get("primary_batch", {}).get("batch_id")
            if promotion_outcome_context is not None and isinstance(promotion_outcome_context.manifest.get("primary_batch"), dict)
            else primary_bundle.manifest.batch_id
        ) or "n/a",
    )
    promotion_context_columns[4].metric(
        "Secondary Batch",
        (
            promotion_outcome_context.manifest.get("secondary_batch", {}).get("batch_id")
            if promotion_outcome_context is not None and isinstance(promotion_outcome_context.manifest.get("secondary_batch"), dict)
            else secondary_bundle.manifest.batch_id
        ) or "n/a",
    )
    promotion_context_columns[5].metric(
        "Selected Metric",
        _metric_label(
            _optional_str(promotion_outcome_context.manifest.get("selected_metric"))
            if promotion_outcome_context is not None
            else selected_metric
        ),
    )
    promotion_context_columns[6].metric(
        "Decision Status",
        promotion_outcome_context.decision_status if promotion_outcome_context is not None else "n/a",
    )
    promotion_context_columns[7].metric("Common DOIs", common_doi_count)

    if promotion_candidate_load_error:
        st.warning(promotion_candidate_load_error)
    if promotion_outcome_load_error:
        st.warning(promotion_outcome_load_error)
    for message in promotion_compatibility:
        st.warning(message)
    if promotion_guardrail is not None:
        _render_guardrail_panel(promotion_guardrail, title="Promotion Guardrails")
    if promotion_requires_override:
        st.warning(
            "This outcome did not pass the evidence guardrails. Promotion is still possible, "
            "but it now requires explicit override acknowledgment and rationale."
        )

    with st.form("ui_baseline_promotion_form", clear_on_submit=False):
        st.text_input(
            "Candidate Run Directory",
            placeholder="runs/comparisons/comparison_001/review_packets/packet_001/candidate_runs/candidate_001",
            key="ui_promotion_candidate_run_dir",
        )
        st.text_input(
            "Outcome Directory",
            placeholder="runs/comparisons/comparison_001/review_packets/packet_001/candidate_runs/candidate_001/outcomes/outcome_accept_001",
            key="ui_promotion_outcome_dir",
        )
        promotion_identity_columns = st.columns([1.1, 1.0])
        with promotion_identity_columns[0]:
            st.text_input("Baseline ID", key="ui_promotion_baseline_id")
        with promotion_identity_columns[1]:
            st.text_input("Reviewer", key="ui_promotion_reviewer")
        st.text_area("Notes / rationale", key="ui_promotion_notes", height=120)
        if promotion_requires_override:
            st.checkbox(
                "I understand the guardrails and still want to promote this baseline",
                key="ui_promotion_override_used",
            )
            st.text_area(
                "Override rationale",
                key="ui_promotion_override_reason",
                height=100,
            )
        promotion_submitted = st.form_submit_button(
            "Promote Accepted Baseline",
            type="primary",
            disabled=(
                not bool(str(st.session_state.get("ui_promotion_candidate_run_dir", "")).strip())
                or not bool(str(st.session_state.get("ui_promotion_outcome_dir", "")).strip())
            ),
        )

    if promotion_submitted:
        _submit_baseline_promotion(
            primary_bundle=primary_bundle,
            secondary_bundle=secondary_bundle,
            guardrail_assessment=promotion_guardrail,
        )

    promotion_error = st.session_state.get("ui_promotion_error")
    if promotion_error:
        st.error(promotion_error)

    promotion_result = st.session_state.get("ui_promotion_result")
    if isinstance(promotion_result, BaselinePromotionResult):
        st.success(f"Promoted accepted baseline to {_display_path(promotion_result.baseline_dir)}")
        st.markdown(f"**Accepted Theory Snapshot:** `{_display_path(promotion_result.accepted_theory_snapshot_path)}`")
        with st.expander("Accepted baseline artifact paths"):
            st.json(
                {
                    "accepted_baseline_manifest.json": _display_path(promotion_result.manifest_path),
                    "accepted_theory_snapshot.yaml": _display_path(promotion_result.accepted_theory_snapshot_path),
                    "candidate_reply.yaml": _display_path(promotion_result.candidate_reply_path),
                    "applied_changes.jsonl": _display_path(promotion_result.applied_changes_path),
                    "promotion_record.json": _display_path(promotion_result.promotion_record_path),
                }
            )


def _render_candidate_reply_tab() -> None:
    st.subheader("Candidate Reply")
    st.caption(
        "Load a review packet and preview a candidate reply against the existing "
        "`apply-generator-reply` schema before optionally materializing a copied theory "
        "snapshot and running a new candidate batch."
    )

    packet_columns = st.columns([4.0, 1.2])
    with packet_columns[0]:
        st.text_input(
            "Review packet directory",
            placeholder="runs/comparisons/comparison_001/review_packets/packet_001",
            key="ui_reply_packet_dir_input",
        )
    with packet_columns[1]:
        st.write("")
        if st.button("Load Packet", use_container_width=True):
            _load_review_packet_from_ui()

    packet_error = st.session_state.get("ui_reply_packet_error")
    if packet_error:
        st.error(packet_error)

    packet_bundle = st.session_state.get("ui_loaded_reply_packet")
    if not isinstance(packet_bundle, ReviewPacketBundle):
        st.info("Load a review packet directory to inspect its baseline snapshot and reply constraints.")
        return

    manifest = packet_bundle.manifest
    primary_batch = manifest.get("primary_batch") if isinstance(manifest.get("primary_batch"), dict) else {}
    packet_context_columns = st.columns(5)
    packet_context_columns[0].metric("Packet ID", manifest.get("packet_id") or "n/a")
    packet_context_columns[1].metric("Comparison ID", manifest.get("comparison_id") or "n/a")
    packet_context_columns[2].metric("Primary Batch", primary_batch.get("batch_id") or "n/a")
    packet_context_columns[3].metric("Metric", _metric_label(manifest.get("selected_packet_metric")))
    packet_context_columns[4].metric("Allowed Paths", len(packet_bundle.allowed_revision_paths))
    st.markdown(f"**Baseline Theory Snapshot:** `{_display_path(packet_bundle.baseline_snapshot_path)}`")
    if packet_bundle.template_path is not None:
        st.markdown(f"**Template File:** `{_display_path(packet_bundle.template_path)}`")

    reply_columns = st.columns([4.0, 1.3])
    with reply_columns[0]:
        st.text_input(
            "Candidate reply YAML path",
            placeholder="runs/comparisons/comparison_001/review_packets/packet_001/my_reply.yaml",
            key="ui_candidate_reply_path",
        )
    with reply_columns[1]:
        st.write("")
        if st.button("Load / Preview Reply", use_container_width=True):
            _preview_candidate_reply_from_ui()

    preview_error = st.session_state.get("ui_reply_preview_error")
    if preview_error:
        st.error(preview_error)

    preview_result = st.session_state.get("ui_reply_preview_result")
    if not isinstance(preview_result, ReplyPreviewResult):
        st.info("Load or preview a candidate reply YAML to validate it against the loaded review packet.")
        return

    if preview_result.state == "valid":
        st.success("Reply state: VALID")
    elif preview_result.state == "template_only":
        st.warning("Reply state: TEMPLATE-ONLY")
    else:
        st.error("Reply state: INVALID")

    summary_columns = st.columns(4)
    summary_columns[0].metric("Proposed Changes", preview_result.proposed_change_count)
    summary_columns[1].metric("Valid Changes", preview_result.valid_change_count)
    summary_columns[2].metric("Invalid Changes", preview_result.invalid_change_count)
    summary_columns[3].metric("Selected Metric", _metric_label(preview_result.selected_metric))
    st.markdown(f"**Reply Path:** `{_display_path(preview_result.reply_path)}`")

    if preview_result.errors:
        st.markdown("**Validation Errors**")
        for message in preview_result.errors:
            st.write(f"- {message}")
    if preview_result.warnings:
        st.markdown("**Warnings**")
        for message in preview_result.warnings:
            st.write(f"- {message}")

    st.subheader("Scalar Diff")
    if preview_result.diff_rows:
        st.dataframe(
            _rows_to_table_data(
                [
                    {
                        "path": row.get("path"),
                        "baseline_value": row.get("baseline_value"),
                        "proposed_value": row.get("proposed_value"),
                        "scalar_type": row.get("scalar_type"),
                        "numeric_delta": row.get("numeric_delta"),
                        "status": row.get("status"),
                        "note": row.get("note"),
                    }
                    for row in preview_result.diff_rows
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No diff rows are available for the current reply preview.")

    st.subheader("Grouped Summary")
    if preview_result.grouped_summary:
        st.dataframe(
            _rows_to_table_data(preview_result.grouped_summary),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No grouped change summary is available for the current reply preview.")

    st.subheader("Apply Candidate & Run Batch")
    primary_bundle = st.session_state.get("ui_loaded_batch")
    source_primary_bundle = primary_bundle if isinstance(primary_bundle, BatchUiBundle) else None
    selected_metric = _optional_str(packet_bundle.manifest.get("selected_packet_metric"))
    precondition_errors = _candidate_apply_precondition_errors(
        primary_bundle=source_primary_bundle,
        packet_bundle=packet_bundle,
        preview_result=preview_result,
    )

    source_batch_id = (
        source_primary_bundle.manifest.batch_id
        if source_primary_bundle is not None
        else primary_batch.get("batch_id") or "n/a"
    )
    source_batch_dir = (
        _display_path(source_primary_bundle.batch_dir)
        if source_primary_bundle is not None
        else _display_path(primary_batch.get("batch_dir"))
    )
    source_seeds_csv = (
        _display_path(source_primary_bundle.manifest.seeds_csv)
        if source_primary_bundle is not None
        else "n/a"
    )
    source_options = (
        _batch_option_display_values(getattr(source_primary_bundle.manifest, "options", None))
        if source_primary_bundle is not None
        else None
    )

    apply_context_columns = st.columns(5)
    apply_context_columns[0].metric("Packet ID", manifest.get("packet_id") or "n/a")
    apply_context_columns[1].metric("Comparison ID", manifest.get("comparison_id") or "n/a")
    apply_context_columns[2].metric("Primary Batch", source_batch_id)
    apply_context_columns[3].metric("Packet Metric", _metric_label(selected_metric))
    apply_context_columns[4].metric("Reply State", preview_result.state.upper())

    st.markdown(f"**Baseline Theory Snapshot:** `{_display_path(packet_bundle.baseline_snapshot_path)}`")
    st.markdown(f"**Source Primary Batch Dir:** `{source_batch_dir}`")
    st.markdown(f"**Source Seeds CSV:** `{source_seeds_csv}`")
    if source_options is not None:
        with st.expander("Effective Batch Options To Reuse"):
            st.json(source_options)
    else:
        st.info("Primary batch parity options are unavailable until a compatible primary batch is loaded.")

    if precondition_errors:
        st.warning("Apply/run is currently blocked:")
        for message in precondition_errors:
            st.write(f"- {message}")

    with st.form("ui_candidate_apply_form", clear_on_submit=False):
        apply_identity_columns = st.columns([1.0, 1.0, 1.0])
        with apply_identity_columns[0]:
            st.text_input("Candidate ID", key="ui_candidate_apply_id")
        with apply_identity_columns[1]:
            st.text_input("Output Batch ID", key="ui_candidate_output_batch_id")
        with apply_identity_columns[2]:
            st.text_input("Reviewer", key="ui_candidate_apply_reviewer")
        st.text_area("Notes", key="ui_candidate_apply_notes", height=100)
        apply_submitted = st.form_submit_button(
            "Apply Candidate & Run Batch",
            type="primary",
            disabled=bool(precondition_errors) or bool(st.session_state.get("ui_candidate_apply_in_progress", False)),
        )

    if apply_submitted:
        _submit_candidate_apply_and_run(primary_bundle=source_primary_bundle)

    candidate_apply_error = st.session_state.get("ui_candidate_apply_error")
    if candidate_apply_error:
        st.error(candidate_apply_error)
    candidate_apply_warning = st.session_state.get("ui_candidate_apply_warning")
    if candidate_apply_warning:
        st.warning(candidate_apply_warning)

    candidate_apply_result = st.session_state.get("ui_candidate_apply_result")
    if isinstance(candidate_apply_result, CandidateApplyRunResult):
        if candidate_apply_result.status == "completed":
            st.success(
                f"Candidate batch '{candidate_apply_result.output_batch_id}' completed and is now loaded as the secondary comparison batch."
            )
        else:
            st.warning(
                f"Candidate batch '{candidate_apply_result.output_batch_id}' failed after candidate artifacts were written."
            )

        result_columns = st.columns(5)
        result_columns[0].metric("Candidate ID", candidate_apply_result.candidate_id)
        result_columns[1].metric("Output Batch ID", candidate_apply_result.output_batch_id)
        result_columns[2].metric("Status", candidate_apply_result.status)
        result_columns[3].metric(
            "Completed Seeds",
            (
                candidate_apply_result.batch_summary.completed_seed_count
                if candidate_apply_result.batch_summary is not None
                and candidate_apply_result.batch_summary.completed_seed_count is not None
                else "n/a"
            ),
        )
        result_columns[4].metric(
            "Failed Seeds",
            (
                candidate_apply_result.batch_summary.failed_seed_count
                if candidate_apply_result.batch_summary is not None
                and candidate_apply_result.batch_summary.failed_seed_count is not None
                else "n/a"
            ),
        )
        st.markdown(f"**Candidate Dir:** `{_display_path(candidate_apply_result.candidate_dir)}`")
        st.markdown(f"**Output Batch Dir:** `{_display_path(candidate_apply_result.output_batch_dir)}`")

        with st.expander("Candidate Artifact Paths"):
            st.json(
                {
                    "candidate_apply_manifest.json": _display_path(candidate_apply_result.manifest_path),
                    "candidate_reply.yaml": _display_path(candidate_apply_result.copied_reply_path),
                    "candidate_theory_snapshot.yaml": _display_path(candidate_apply_result.candidate_theory_snapshot_path),
                    "applied_changes.jsonl": _display_path(candidate_apply_result.applied_changes_path),
                    "batch_run_request.json": _display_path(candidate_apply_result.batch_run_request_path),
                    "batch_run_result.json": _display_path(candidate_apply_result.batch_run_result_path),
                }
            )


def _metric_summary_rows(bundle: BatchUiBundle) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for metric_name, label in METRIC_LABELS.items():
        stats = bundle.aggregate_summary.metric_aggregates.get(metric_name)
        rows.append(
            {
                "metric": label,
                "count": stats.count if stats is not None else 0,
                "mean": stats.mean if stats is not None else None,
                "median": stats.median if stats is not None else None,
                "std": stats.std if stats is not None else None,
                "spread": stats.spread if stats is not None else None,
            }
        )
    return rows


def _filter_seed_rows(
    rows: list[dict[str, Any]],
    *,
    status_mode: str,
    doi_filter: str,
    sort_metric: str,
    descending: bool,
) -> list[dict[str, Any]]:
    filtered_rows = rows
    lowered_filter = doi_filter.strip().lower()
    if status_mode == "completed only":
        filtered_rows = [row for row in filtered_rows if row.get("status") == "completed"]
    elif status_mode == "failed only":
        filtered_rows = [row for row in filtered_rows if row.get("status") == "failed"]

    if lowered_filter:
        filtered_rows = [
            row for row in filtered_rows
            if lowered_filter in str(row.get("doi", "")).lower()
        ]

    if sort_metric == "batch_index":
        return sorted(
            filtered_rows,
            key=lambda row: int(row.get("batch_index", 0)),
            reverse=descending,
        )

    populated_rows = [row for row in filtered_rows if row.get(sort_metric) is not None]
    missing_rows = [row for row in filtered_rows if row.get(sort_metric) is None]
    populated_rows.sort(
        key=lambda row: (row.get(sort_metric), int(row.get("batch_index", 0))),
        reverse=descending,
    )
    missing_rows.sort(key=lambda row: int(row.get("batch_index", 0)))
    return populated_rows + missing_rows


def _seed_sort_options() -> list[str]:
    return ["batch_index", *METRIC_LABELS.keys()]


def _seed_table_display_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    display_rows: list[dict[str, Any]] = []
    for row in rows:
        display_rows.append(
            {
                column_name: _display_path(row[column_name]) if column_name == "run_dir" else row.get(column_name)
                for column_name in SEED_TABLE_COLUMNS
            }
        )
    return display_rows


def _failed_seed_display_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "batch_index": row.get("batch_index"),
            "doi": row.get("doi"),
            "status": row.get("status"),
            "error_type": row.get("error_type"),
            "error_message": row.get("error_message"),
            "run_dir": _display_path(row.get("run_dir")),
        }
        for row in rows
    ]


def _render_ranked_seed_group(rows: list[dict[str, Any]], *, empty_message: str) -> None:
    if not rows:
        st.info(empty_message)
        return

    st.dataframe(
        _rows_to_table_data(
            [
                {
                    "batch_index": row.get("batch_index"),
                    "doi": row.get("doi"),
                    "ranking_metric": row.get("ranking_metric"),
                    "ranking_value": row.get("ranking_value"),
                    "precision_at_k": row.get("precision_at_k"),
                    "recall_at_k": row.get("recall_at_k"),
                    "ndcg_at_k": row.get("ndcg_at_k"),
                    "brier_score": row.get("brier_score"),
                    "expected_calibration_error": row.get("expected_calibration_error"),
                }
                for row in rows
            ]
        ),
        use_container_width=True,
        hide_index=True,
    )

    for row in rows:
        title = f"{row.get('batch_index', '?')}: {row.get('doi', 'unknown')} ({row.get('ranking_metric', 'metric')}={row.get('ranking_value')})"
        with st.expander(title):
            st.json(row)


def _artifact_paths_for_seed(seed_row: dict[str, Any]) -> dict[str, Any]:
    artifact_keys = (
        "run_dir",
        "corpus_manifest_json",
        "silver_labels_csv",
        "experiment_dir",
        "experiment_manifest_json",
        "evaluation_summary_json",
        "evaluation_cases_json",
    )
    return {
        key: _display_path(value)
        for key, value in ((key, seed_row.get(key)) for key in artifact_keys)
        if value
    }


def _submit_run_batch_form() -> None:
    selected_theory_config = _get_selected_theory_config_from_widget()
    st.session_state[THEORY_CONFIG_STATE_KEY] = selected_theory_config
    try:
        request = build_batch_run_request(
            initial_doi_context=st.session_state.get("ui_initial_doi_context", ""),
            theory_config_path=selected_theory_config,
            seeds_csv_path=st.session_state.get("ui_seeds_csv_path", ""),
            batch_id=st.session_state.get("ui_run_batch_id", ""),
            max_references=st.session_state.get("ui_run_max_references", DEFAULT_BATCH_MAX_REFERENCES),
            max_related=st.session_state.get("ui_run_max_related", DEFAULT_BATCH_MAX_RELATED),
            max_hard_negatives=st.session_state.get("ui_run_max_hard_negatives", DEFAULT_BATCH_MAX_HARD_NEGATIVES),
            top_k=st.session_state.get("ui_run_top_k", DEFAULT_BATCH_TOP_K),
            label_source=st.session_state.get("ui_run_label_source", SUPPORTED_LABEL_SOURCES[0]),
            refresh=bool(st.session_state.get("ui_run_refresh", False)),
        )
    except BatchRunValidationError as exc:
        st.session_state["ui_last_run_error"] = str(exc)
        st.session_state["ui_last_run_warning"] = None
        st.session_state["ui_last_run_summary"] = None
        st.session_state["ui_partial_run_bundle"] = None
        st.session_state["ui_partial_run_summary"] = None
        return

    run_context_payload = _build_ui_run_context_payload(
        request,
        launch_source_type="run_batch_form",
    )
    _execute_batch_run_request(
        request,
        run_context_payload=run_context_payload,
        warning_state_key="ui_last_run_warning",
    )


def _submit_save_benchmark_preset() -> None:
    try:
        request = build_benchmark_preset_save_request(
            preset_id=str(st.session_state.get("ui_new_benchmark_preset_id", "")),
            seeds_csv_path=str(st.session_state.get("ui_new_benchmark_preset_seeds_csv", "")),
            description=str(st.session_state.get("ui_new_benchmark_preset_description", "")),
            tags_text=str(st.session_state.get("ui_new_benchmark_preset_tags", "")),
        )
        preset_path = save_benchmark_preset(request)
    except PresetRegistryError as exc:
        st.session_state["ui_benchmark_preset_error"] = str(exc)
        st.session_state["ui_benchmark_preset_saved_path"] = None
        return

    st.session_state["ui_benchmark_preset_error"] = None
    st.session_state["ui_benchmark_preset_saved_path"] = _display_path(preset_path)
    st.session_state["ui_registry_selected_benchmark_preset_id"] = request.preset_id
    st.rerun()


def _submit_save_curated_benchmark_preset() -> None:
    try:
        request = build_curated_benchmark_preset_save_request(
            preset_id=str(st.session_state.get("ui_curation_preset_id", "")),
            curation_dir=str(st.session_state.get("ui_curation_preset_dir", "")),
            description=str(st.session_state.get("ui_curation_preset_description", "")),
            tags_text=str(st.session_state.get("ui_curation_preset_tags", "")),
        )
        curation = load_curation_bundle_context(request.curation_dir)
        preset_path = save_curated_benchmark_preset(request, curation=curation)
    except CuratedBenchmarkPresetError as exc:
        st.session_state["ui_curation_preset_error"] = str(exc)
        st.session_state["ui_curation_preset_saved_path"] = None
        return

    st.session_state["ui_curation_preset_error"] = None
    st.session_state["ui_curation_preset_saved_path"] = _display_path(preset_path)
    st.session_state["ui_registry_selected_benchmark_preset_id"] = request.preset_id

    if bool(st.session_state.get("ui_curation_preset_select_for_run_batch", False)):
        entries, _ = scan_benchmark_presets()
        entry = find_benchmark_preset_entry(entries, request.preset_id)
        if entry is not None:
            _select_benchmark_preset_for_run_batch(entry)

    st.rerun()


def _submit_save_evaluation_preset() -> None:
    try:
        request = build_evaluation_preset_save_request(
            preset_id=str(st.session_state.get("ui_new_eval_preset_id", "")),
            max_references=int(st.session_state.get("ui_new_eval_max_references", DEFAULT_BATCH_MAX_REFERENCES)),
            max_related=int(st.session_state.get("ui_new_eval_max_related", DEFAULT_BATCH_MAX_RELATED)),
            max_hard_negatives=int(st.session_state.get("ui_new_eval_max_hard_negatives", DEFAULT_BATCH_MAX_HARD_NEGATIVES)),
            top_k=int(st.session_state.get("ui_new_eval_top_k", DEFAULT_BATCH_TOP_K)),
            label_source=str(st.session_state.get("ui_new_eval_label_source", SUPPORTED_LABEL_SOURCES[0])),
            refresh=bool(st.session_state.get("ui_new_eval_refresh", False)),
            description=str(st.session_state.get("ui_new_eval_preset_description", "")),
        )
        preset_path = save_evaluation_preset(request)
    except PresetRegistryError as exc:
        st.session_state["ui_eval_preset_error"] = str(exc)
        st.session_state["ui_eval_preset_saved_path"] = None
        return

    st.session_state["ui_eval_preset_error"] = None
    st.session_state["ui_eval_preset_saved_path"] = _display_path(preset_path)
    st.session_state["ui_registry_selected_eval_preset_id"] = request.preset_id
    st.rerun()


def _submit_save_launch_profile() -> None:
    context = _current_launch_profile_selection_context()
    selected_eval_values = context.get("selected_eval_values")
    try:
        request = build_launch_profile_save_request(
            profile_id=str(st.session_state.get("ui_new_launch_profile_id", "")),
            accepted_baseline_id=context.get("accepted_baseline_id") or "",
            accepted_baseline_dir=context.get("accepted_baseline_dir") or "",
            accepted_theory_snapshot=context.get("accepted_theory_snapshot") or "",
            benchmark_preset_id=context.get("benchmark_preset_id") or "",
            seeds_csv=context.get("seeds_csv") or "",
            eval_preset_id=context.get("eval_preset_id") or "",
            max_references=(selected_eval_values or {}).get("max_references", DEFAULT_BATCH_MAX_REFERENCES),
            max_related=(selected_eval_values or {}).get("max_related", DEFAULT_BATCH_MAX_RELATED),
            max_hard_negatives=(selected_eval_values or {}).get(
                "max_hard_negatives",
                DEFAULT_BATCH_MAX_HARD_NEGATIVES,
            ),
            top_k=(selected_eval_values or {}).get("top_k", DEFAULT_BATCH_TOP_K),
            label_source=_optional_str((selected_eval_values or {}).get("label_source")) or "",
            refresh=bool((selected_eval_values or {}).get("refresh", False)),
            description=str(st.session_state.get("ui_new_launch_profile_description", "")),
            tags_text=str(st.session_state.get("ui_new_launch_profile_tags", "")),
        )
        profile_path = save_launch_profile(request)
    except LaunchProfileRegistryError as exc:
        st.session_state["ui_launch_profile_error"] = str(exc)
        st.session_state["ui_launch_profile_saved_path"] = None
        return

    st.session_state["ui_launch_profile_error"] = None
    st.session_state["ui_launch_profile_saved_path"] = _display_path(profile_path)
    st.session_state["ui_registry_selected_launch_profile_id"] = request.profile_id
    st.rerun()


def _submit_run_launch_profile(entry: LaunchProfileEntry) -> None:
    values, warnings = build_launch_profile_run_batch_values(
        entry,
        allowed_label_sources=SUPPORTED_LABEL_SOURCES,
        fallback_label_source=SUPPORTED_LABEL_SOURCES[0],
    )
    try:
        request = build_batch_run_request(
            initial_doi_context=st.session_state.get("ui_launch_profile_run_initial_doi_context", ""),
            theory_config_path=values.get("theory_config_path", ""),
            seeds_csv_path=values.get("seeds_csv_path", ""),
            batch_id=st.session_state.get("ui_launch_profile_run_batch_id", ""),
            max_references=values.get("max_references", DEFAULT_BATCH_MAX_REFERENCES),
            max_related=values.get("max_related", DEFAULT_BATCH_MAX_RELATED),
            max_hard_negatives=values.get("max_hard_negatives", DEFAULT_BATCH_MAX_HARD_NEGATIVES),
            top_k=values.get("top_k", DEFAULT_BATCH_TOP_K),
            label_source=values.get("label_source", SUPPORTED_LABEL_SOURCES[0]),
            refresh=bool(values.get("refresh", False)),
        )
    except BatchRunValidationError as exc:
        st.session_state["ui_launch_profile_run_error"] = "\n".join(
            warning for warning in [*warnings, str(exc)] if warning
        )
        st.session_state["ui_launch_profile_run_warning"] = None
        st.session_state["ui_launch_profile_run_summary"] = None
        return

    run_context_payload = _build_ui_run_context_payload(
        request,
        launch_source_type="launch_profile",
        launch_profile_entry=entry,
    )
    outcome = _execute_batch_run_request(
        request,
        run_context_payload=run_context_payload,
        warning_state_key="ui_launch_profile_run_warning",
    )
    st.session_state["ui_launch_profile_run_error"] = outcome.error_message
    st.session_state["ui_launch_profile_run_summary"] = outcome.summary


def _current_launch_profile_selection_context(
    session_state: MutableMapping[str, Any] | None = None,
) -> dict[str, Any]:
    state = st.session_state if session_state is None else session_state
    selected_eval_values = state.get("ui_selected_eval_preset_values")
    return {
        "accepted_baseline_id": _optional_str(state.get("ui_selected_accepted_baseline_id")),
        "accepted_baseline_dir": _optional_str(state.get("ui_selected_accepted_baseline_dir")),
        "accepted_theory_snapshot": _optional_str(state.get("ui_selected_accepted_baseline_theory_path")),
        "benchmark_preset_id": _optional_str(state.get("ui_selected_benchmark_preset_id")),
        "seeds_csv": _optional_str(state.get("ui_selected_benchmark_preset_seeds_csv")),
        "eval_preset_id": _optional_str(state.get("ui_selected_eval_preset_id")),
        "selected_eval_values": dict(selected_eval_values) if isinstance(selected_eval_values, dict) else None,
        "label_source": (
            _optional_str((selected_eval_values or {}).get("label_source"))
            if isinstance(selected_eval_values, dict)
            else None
        ),
    }


def _build_ui_run_context_payload(
    request: Any,
    *,
    launch_source_type: str,
    launch_profile_entry: LaunchProfileEntry | None = None,
    candidate_lineage: dict[str, Any] | None = None,
    study_source: dict[str, Any] | None = None,
    reviewer: str | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    return build_run_context_payload_from_request(
        request,
        launch_source_type=launch_source_type,
        accepted_baseline=_accepted_baseline_run_context_for_request(
            request,
            launch_profile_entry=launch_profile_entry,
        ),
        benchmark_preset=_benchmark_preset_run_context_for_request(
            request,
            launch_profile_entry=launch_profile_entry,
        ),
        evaluation_preset=_evaluation_preset_run_context_for_request(
            request,
            launch_profile_entry=launch_profile_entry,
        ),
        launch_profile=_launch_profile_run_context_for_request(
            request,
            launch_profile_entry=launch_profile_entry,
        ),
        candidate_lineage=candidate_lineage,
        study_source=study_source,
        reviewer=reviewer,
        notes=notes,
    )


def _accepted_baseline_run_context_for_request(
    request: Any,
    *,
    launch_profile_entry: LaunchProfileEntry | None = None,
) -> dict[str, Any] | None:
    if launch_profile_entry is not None:
        accepted_theory_snapshot = _optional_str(launch_profile_entry.payload.get("accepted_theory_snapshot"))
        if _normalized_path_string(accepted_theory_snapshot) != _normalized_path_string(
            getattr(request, "theory_config_path", None)
        ):
            return None
        return {
            "accepted_baseline_id": _optional_str(launch_profile_entry.payload.get("accepted_baseline_id")),
            "accepted_baseline_dir": _optional_str(launch_profile_entry.payload.get("accepted_baseline_dir")),
            "accepted_theory_snapshot": accepted_theory_snapshot,
        }

    accepted_baseline_id = _optional_str(st.session_state.get("ui_selected_accepted_baseline_id"))
    accepted_theory_snapshot = _optional_str(st.session_state.get("ui_selected_accepted_baseline_theory_path"))
    if accepted_baseline_id is None or accepted_theory_snapshot is None:
        return None
    if _normalized_path_string(accepted_theory_snapshot) != _normalized_path_string(
        getattr(request, "theory_config_path", None)
    ):
        return None
    return {
        "accepted_baseline_id": accepted_baseline_id,
        "accepted_baseline_dir": _optional_str(st.session_state.get("ui_selected_accepted_baseline_dir")),
        "accepted_theory_snapshot": accepted_theory_snapshot,
    }


def _benchmark_preset_run_context_for_request(
    request: Any,
    *,
    launch_profile_entry: LaunchProfileEntry | None = None,
) -> dict[str, Any] | None:
    selected_context = _selected_benchmark_preset_run_context_for_request(request)
    if launch_profile_entry is None:
        return selected_context

    profile_benchmark_id = _optional_str(launch_profile_entry.payload.get("benchmark_preset_id"))
    if profile_benchmark_id is None:
        return None
    if selected_context is not None and selected_context.get("benchmark_preset_id") == profile_benchmark_id:
        return selected_context

    context: dict[str, Any] = {"benchmark_preset_id": profile_benchmark_id}
    registry_entry = _resolve_benchmark_preset_entry(profile_benchmark_id)
    if registry_entry is not None and _normalized_path_string(registry_entry.seeds_csv_path) == _normalized_path_string(
        getattr(request, "seeds_csv_path", None)
    ):
        context["benchmark_preset_path"] = _display_path(registry_entry.preset_path)
        context.update(_benchmark_source_lineage_fields(registry_entry.payload))
    return context


def _selected_benchmark_preset_run_context_for_request(
    request: Any,
) -> dict[str, Any] | None:
    preset_id = _optional_str(st.session_state.get("ui_selected_benchmark_preset_id"))
    seeds_csv = _optional_str(st.session_state.get("ui_selected_benchmark_preset_seeds_csv"))
    if preset_id is None or seeds_csv is None:
        return None
    if _normalized_path_string(seeds_csv) != _normalized_path_string(getattr(request, "seeds_csv_path", None)):
        return None

    context: dict[str, Any] = {
        "benchmark_preset_id": preset_id,
        "benchmark_preset_path": _optional_str(st.session_state.get("ui_selected_benchmark_preset_path")),
    }
    registry_entry = _resolve_benchmark_preset_entry(preset_id)
    if registry_entry is not None:
        context.update(_benchmark_source_lineage_fields(registry_entry.payload))
    return context


def _benchmark_source_lineage_fields(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_type": _optional_str(payload.get("source_type")),
        "source_curation_id": _optional_str(payload.get("source_curation_id")),
        "source_curation_dir": _optional_str(payload.get("source_curation_dir")),
    }


def _evaluation_preset_run_context_for_request(
    request: Any,
    *,
    launch_profile_entry: LaunchProfileEntry | None = None,
) -> dict[str, Any] | None:
    selected_context = _selected_evaluation_preset_run_context_for_request(request)
    if launch_profile_entry is None:
        return selected_context

    profile_eval_id = _optional_str(launch_profile_entry.payload.get("eval_preset_id"))
    if profile_eval_id is None:
        return None
    if selected_context is not None and selected_context.get("eval_preset_id") == profile_eval_id:
        return selected_context

    context: dict[str, Any] = {"eval_preset_id": profile_eval_id}
    registry_entry = _resolve_evaluation_preset_entry(profile_eval_id)
    if registry_entry is not None and _request_matches_eval_values(request, registry_entry.payload):
        context["eval_preset_path"] = _display_path(registry_entry.preset_path)
    return context


def _selected_evaluation_preset_run_context_for_request(
    request: Any,
) -> dict[str, Any] | None:
    preset_id = _optional_str(st.session_state.get("ui_selected_eval_preset_id"))
    preset_values = st.session_state.get("ui_selected_eval_preset_values")
    if preset_id is None or not _request_matches_eval_values(request, preset_values):
        return None
    return {
        "eval_preset_id": preset_id,
        "eval_preset_path": _optional_str(st.session_state.get("ui_selected_eval_preset_path")),
    }


def _launch_profile_run_context_for_request(
    request: Any,
    *,
    launch_profile_entry: LaunchProfileEntry | None = None,
) -> dict[str, Any] | None:
    if launch_profile_entry is not None:
        return {
            "launch_profile_id": launch_profile_entry.profile_id,
            "launch_profile_path": _display_path(launch_profile_entry.profile_path),
        }

    launch_profile_id = _optional_str(st.session_state.get("ui_selected_launch_profile_id"))
    selected_values = st.session_state.get("ui_selected_launch_profile_values")
    if launch_profile_id is None or not _request_matches_launch_profile_values(request, selected_values):
        return None
    return {
        "launch_profile_id": launch_profile_id,
        "launch_profile_path": _optional_str(st.session_state.get("ui_selected_launch_profile_path")),
    }


def _request_matches_eval_values(
    request: Any,
    expected_values: Any,
) -> bool:
    if not isinstance(expected_values, dict):
        return False
    required_fields = (
        "max_references",
        "max_related",
        "max_hard_negatives",
        "top_k",
        "label_source",
        "refresh",
    )
    if any(field_name not in expected_values for field_name in required_fields):
        return False
    return (
        getattr(request, "max_references", None) == expected_values.get("max_references")
        and getattr(request, "max_related", None) == expected_values.get("max_related")
        and getattr(request, "max_hard_negatives", None) == expected_values.get("max_hard_negatives")
        and getattr(request, "top_k", None) == expected_values.get("top_k")
        and _optional_str(getattr(request, "label_source", None)) == _optional_str(expected_values.get("label_source"))
        and bool(getattr(request, "refresh", False)) == bool(expected_values.get("refresh", False))
    )


def _request_matches_launch_profile_values(
    request: Any,
    expected_values: Any,
) -> bool:
    if not isinstance(expected_values, dict):
        return False
    return (
        _normalized_path_string(getattr(request, "theory_config_path", None))
        == _normalized_path_string(expected_values.get("theory_config_path"))
        and _normalized_path_string(getattr(request, "seeds_csv_path", None))
        == _normalized_path_string(expected_values.get("seeds_csv_path"))
        and _request_matches_eval_values(request, expected_values)
    )


def _resolve_benchmark_preset_entry(preset_id: str) -> BenchmarkPresetEntry | None:
    entries, _ = scan_benchmark_presets()
    return find_benchmark_preset_entry(entries, preset_id)


def _resolve_evaluation_preset_entry(preset_id: str) -> EvaluationPresetEntry | None:
    entries, _ = scan_evaluation_presets()
    return find_evaluation_preset_entry(entries, preset_id)


def _write_candidate_run_batch_context(
    result: CandidateApplyRunResult,
    *,
    reviewer: str | None,
    notes: str | None,
) -> str | None:
    if not Path(result.output_batch_dir).exists():
        return None
    try:
        study_source_context = load_study_source_from_json_file(result.manifest_path)
        write_run_context(
            result.output_batch_dir,
            {
                **_build_ui_run_context_payload(
                    result.output_batch_request,
                    launch_source_type="candidate_apply_run",
                    candidate_lineage={
                        "candidate_id": result.candidate_id,
                        "packet_id": result.packet_id,
                        "comparison_id": result.comparison_id,
                        "candidate_run_dir": result.candidate_dir,
                    },
                    study_source=study_source_context,
                    reviewer=reviewer,
                    notes=notes,
                ),
                "batch_status": result.status,
                "error_message": _optional_str(result.error_message),
            },
        )
    except RunContextError as exc:
        return str(exc)
    return None


def _execute_batch_run_request(
    request: Any,
    *,
    run_context_payload: dict[str, Any] | None = None,
    warning_state_key: str = "ui_last_run_warning",
) -> Any:
    st.session_state["ui_run_in_progress"] = True
    previous_bundle = st.session_state.get("ui_loaded_batch")
    st.session_state[warning_state_key] = None

    with st.spinner(f"Running batch '{request.batch_id}'..."):
        outcome = run_batch_request(request, previous_bundle=previous_bundle)

    st.session_state["ui_run_in_progress"] = False
    st.session_state["ui_last_run_error"] = outcome.error_message
    st.session_state["ui_last_run_summary"] = outcome.summary
    st.session_state["ui_partial_run_bundle"] = outcome.partial_bundle
    st.session_state["ui_partial_run_summary"] = (
        outcome.summary if outcome.partial_bundle is not None else None
    )

    if run_context_payload is not None and Path(request.batch_dir).exists():
        try:
            write_run_context(
                request.batch_dir,
                {
                    **run_context_payload,
                    "batch_status": (
                        _optional_str(getattr(outcome.summary, "status", None))
                        or ("completed" if outcome.success else "failed")
                    ),
                    "error_message": _optional_str(outcome.error_message),
                },
            )
        except RunContextError as exc:
            st.session_state[warning_state_key] = str(exc)

    if outcome.success and outcome.loaded_bundle is not None:
        _apply_loaded_batch(outcome.loaded_bundle)
        st.session_state["ui_batch_load_error"] = None

    return outcome


def _render_last_run_summary() -> None:
    last_run_error = st.session_state.get("ui_last_run_error")
    if last_run_error:
        st.error(last_run_error)
    last_run_warning = st.session_state.get("ui_last_run_warning")
    if last_run_warning:
        st.warning(last_run_warning)

    summary = st.session_state.get("ui_last_run_summary")
    if summary is not None:
        _render_batch_run_summary(summary)

    partial_bundle = st.session_state.get("ui_partial_run_bundle")
    partial_summary = st.session_state.get("ui_partial_run_summary")
    if partial_bundle is not None and partial_summary is not None:
        st.warning("A partial batch directory is loadable from the failed run attempt.")
        if st.button("Load Partial Batch", use_container_width=False):
            _apply_loaded_batch(partial_bundle)
            st.session_state["ui_last_run_summary"] = partial_summary
            st.session_state["ui_batch_load_error"] = None
            st.session_state["ui_partial_run_bundle"] = None
            st.session_state["ui_partial_run_summary"] = None


def _render_batch_run_summary(summary: BatchRunSummary) -> None:
    st.subheader("Last Run Summary")
    summary_columns = st.columns(5)
    summary_columns[0].metric("Batch ID", summary.batch_id)
    summary_columns[1].metric("Status", summary.status or "n/a")
    summary_columns[2].metric("Seed Count", summary.seed_count if summary.seed_count is not None else "n/a")
    summary_columns[3].metric(
        "Completed",
        summary.completed_seed_count if summary.completed_seed_count is not None else "n/a",
    )
    summary_columns[4].metric(
        "Failed",
        summary.failed_seed_count if summary.failed_seed_count is not None else "n/a",
    )

    st.markdown(f"**Initial DOI UI Context:** `{summary.initial_doi_context or 'n/a'}`")
    st.markdown(f"**Batch Directory:** `{summary.batch_dir}`")
    st.markdown(f"**Theory Config:** `{summary.theory_config}`")
    st.markdown(f"**Seeds CSV:** `{summary.seeds_csv}`")
    if summary.output_paths:
        with st.expander("Output Paths"):
            st.json(summary.output_paths)


def _submit_save_comparison(
    *,
    primary_bundle: BatchUiBundle | None,
    secondary_bundle: BatchUiBundle | None,
    selected_metric: str,
    status_mode: str,
    common_doi_count: int,
    common_completed_seed_count: int,
    compatibility_warning_list: list[str],
    paired_rows: list[dict[str, Any]],
    summary: Any,
) -> None:
    try:
        study_source_assessment = _get_current_study_source_assessment(primary_bundle, secondary_bundle)
        request = build_comparison_save_request(
            comparison_id=str(st.session_state.get("ui_comparison_save_id", "")),
            reviewer=str(st.session_state.get("ui_comparison_reviewer", "")),
            decision_status=str(st.session_state.get("ui_comparison_decision_status", "")),
            notes=str(st.session_state.get("ui_comparison_notes", "")),
        )
        result = save_comparison_artifacts(
            base_dir=REPO_ROOT / "runs" / "comparisons",
            request=request,
            primary_bundle=primary_bundle,
            secondary_bundle=secondary_bundle,
            selected_metric=selected_metric,
            status_mode=status_mode,
            common_doi_count=common_doi_count,
            common_completed_seed_count=common_completed_seed_count,
            compatibility_warning_list=compatibility_warning_list,
            paired_rows=paired_rows,
            summary=summary,
            study_source_context=build_study_source_artifact_fields(study_source_assessment),
        )
    except ComparisonExportError as exc:
        st.session_state["ui_comparison_save_error"] = str(exc)
        st.session_state["ui_comparison_save_result"] = None
        return

    st.session_state["ui_comparison_save_error"] = None
    st.session_state["ui_comparison_save_result"] = result
    _clear_review_packet_feedback()
    st.session_state["ui_review_packet_comparison_id"] = request.comparison_id
    st.session_state["ui_review_packet_reviewer"] = request.reviewer or ""
    if not st.session_state.get("ui_review_packet_metric"):
        st.session_state["ui_review_packet_metric"] = selected_metric


def _clear_comparison_save_feedback(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    state["ui_comparison_save_error"] = None
    state["ui_comparison_save_result"] = None


def _submit_review_packet_export(
    *,
    primary_bundle: BatchUiBundle | None,
    secondary_bundle: BatchUiBundle | None,
    compatibility_warning_list: list[str],
    common_doi_count: int,
    common_completed_seed_count: int,
    paired_rows: list[dict[str, Any]],
    summary: Any,
) -> None:
    try:
        request = build_review_packet_export_request(
            packet_id=str(st.session_state.get("ui_review_packet_id", "")),
            comparison_id=str(st.session_state.get("ui_review_packet_comparison_id", "")),
            reviewer=str(st.session_state.get("ui_review_packet_reviewer", "")),
            selected_metric=str(st.session_state.get("ui_review_packet_metric", "")),
            max_regressions=int(st.session_state.get("ui_review_packet_max_regressions", 10)),
            max_improvements=int(st.session_state.get("ui_review_packet_max_improvements", 10)),
        )
        if summary is None:
            raise ReviewPacketExportError("No usable summary is available for the selected packet metric.")
        result = save_review_packet_artifacts(
            base_dir=REPO_ROOT / "runs" / "comparisons",
            request=request,
            primary_bundle=primary_bundle,
            secondary_bundle=secondary_bundle,
            compatibility_warning_list=compatibility_warning_list,
            common_doi_count=common_doi_count,
            common_completed_seed_count=common_completed_seed_count,
            paired_rows=paired_rows,
            summary=summary,
        )
    except ReviewPacketExportError as exc:
        st.session_state["ui_review_packet_error"] = str(exc)
        st.session_state["ui_review_packet_result"] = None
        return

    st.session_state["ui_review_packet_error"] = None
    st.session_state["ui_review_packet_result"] = result
    st.session_state["ui_reply_packet_dir_input"] = _display_path(result.packet_dir)
    _clear_reply_preview_feedback()


def _candidate_apply_precondition_errors(
    *,
    primary_bundle: BatchUiBundle | None,
    packet_bundle: ReviewPacketBundle | None,
    preview_result: ReplyPreviewResult | None,
) -> list[str]:
    messages: list[str] = []
    if primary_bundle is None:
        messages.append("Load the primary/reference batch in the sidebar before applying a candidate reply.")
    if not isinstance(packet_bundle, ReviewPacketBundle):
        messages.append("Load a review packet before applying a candidate reply.")
    if not isinstance(preview_result, ReplyPreviewResult):
        messages.append("Preview a candidate reply before applying it.")
    elif preview_result.state == "template_only":
        messages.append("Template-only replies cannot be applied.")
    elif preview_result.state != "valid":
        messages.append("Only VALID candidate reply previews can be applied.")

    if isinstance(packet_bundle, ReviewPacketBundle):
        messages.extend(packet_primary_compatibility_errors(packet_bundle, primary_bundle))
    return list(dict.fromkeys(messages))


def _submit_candidate_apply_and_run(
    *,
    primary_bundle: BatchUiBundle | None,
) -> None:
    packet_bundle = st.session_state.get("ui_loaded_reply_packet")
    preview_result = st.session_state.get("ui_reply_preview_result")
    precondition_errors = _candidate_apply_precondition_errors(
        primary_bundle=primary_bundle,
        packet_bundle=packet_bundle if isinstance(packet_bundle, ReviewPacketBundle) else None,
        preview_result=preview_result if isinstance(preview_result, ReplyPreviewResult) else None,
    )
    if precondition_errors:
        st.session_state["ui_candidate_apply_error"] = precondition_errors[0]
        st.session_state["ui_candidate_apply_warning"] = None
        st.session_state["ui_candidate_apply_result"] = None
        return

    if not isinstance(packet_bundle, ReviewPacketBundle):
        st.session_state["ui_candidate_apply_error"] = "Load a review packet before applying a candidate reply."
        st.session_state["ui_candidate_apply_warning"] = None
        st.session_state["ui_candidate_apply_result"] = None
        return

    reply_path = str(st.session_state.get("ui_candidate_reply_path", "")).strip()
    if not reply_path:
        st.session_state["ui_candidate_apply_error"] = "Enter a candidate reply YAML path."
        st.session_state["ui_candidate_apply_warning"] = None
        st.session_state["ui_candidate_apply_result"] = None
        return

    st.session_state["ui_candidate_apply_in_progress"] = True
    st.session_state["ui_candidate_apply_warning"] = None
    try:
        request = build_candidate_apply_run_request(
            candidate_id=str(st.session_state.get("ui_candidate_apply_id", "")),
            output_batch_id=str(st.session_state.get("ui_candidate_output_batch_id", "")),
            reviewer=str(st.session_state.get("ui_candidate_apply_reviewer", "")),
            notes=str(st.session_state.get("ui_candidate_apply_notes", "")),
        )
    except CandidateApplyRunError as exc:
        st.session_state["ui_candidate_apply_error"] = str(exc)
        st.session_state["ui_candidate_apply_warning"] = None
        st.session_state["ui_candidate_apply_result"] = None
        st.session_state["ui_candidate_apply_in_progress"] = False
        return

    selected_metric = _optional_str(packet_bundle.manifest.get("selected_packet_metric"))
    previous_secondary_bundle = st.session_state.get("ui_comparison_secondary_bundle")
    with st.spinner(f"Applying '{request.candidate_id}' and running batch '{request.output_batch_id}'..."):
        try:
            result = run_candidate_apply_and_batch(
                request=request,
                packet_bundle=packet_bundle,
                reply_path=reply_path,
                primary_bundle=primary_bundle,
                previous_secondary_bundle=previous_secondary_bundle if isinstance(previous_secondary_bundle, BatchUiBundle) else None,
                selected_metric=selected_metric,
            )
        except CandidateApplyRunError as exc:
            st.session_state["ui_candidate_apply_error"] = str(exc)
            st.session_state["ui_candidate_apply_warning"] = None
            st.session_state["ui_candidate_apply_result"] = None
            st.session_state["ui_candidate_apply_in_progress"] = False
            return

    st.session_state["ui_candidate_apply_in_progress"] = False
    st.session_state["ui_candidate_apply_error"] = result.error_message
    st.session_state["ui_candidate_apply_warning"] = _write_candidate_run_batch_context(
        result,
        reviewer=request.reviewer,
        notes=request.notes,
    )
    st.session_state["ui_candidate_apply_result"] = result
    _clear_reeval_outcome_feedback()
    _clear_baseline_promotion_feedback()

    if result.status == "completed" and isinstance(result.loaded_secondary_bundle, BatchUiBundle):
        _apply_secondary_comparison_bundle(result.loaded_secondary_bundle)
        st.rerun()


def _sync_reeval_metric_state(default_metric: str, common_metrics: list[str]) -> None:
    if not common_metrics:
        st.session_state["ui_reeval_metric"] = ""
        return

    current_metric = st.session_state.get("ui_reeval_metric")
    if current_metric not in common_metrics:
        st.session_state["ui_reeval_metric"] = (
            default_metric if default_metric in common_metrics else common_metrics[0]
        )


def _load_candidate_run_context_for_display(
    candidate_run_dir: str,
) -> tuple[CandidateRunContext | None, str | None]:
    if not candidate_run_dir.strip():
        return None, None
    try:
        return load_candidate_run_context(candidate_run_dir), None
    except ReevalOutcomeExportError as exc:
        return None, str(exc)


def _load_outcome_context_for_display(
    outcome_dir: str,
) -> tuple[OutcomeContext | None, str | None]:
    if not outcome_dir.strip():
        return None, None
    try:
        return load_outcome_context(outcome_dir), None
    except BaselinePromotionError as exc:
        return None, str(exc)


def _guardrail_assessment_from_outcome_context(
    outcome: OutcomeContext | None,
) -> DecisionGuardrailAssessment | None:
    if outcome is None:
        return None
    return evaluate_decision_guardrails(
        selected_metric=_optional_str(outcome.manifest.get("selected_metric"))
        or _optional_str(outcome.decision_record.get("selected_metric")),
        common_doi_count=int(outcome.manifest.get("common_doi_count") or 0),
        common_completed_seed_count=int(outcome.manifest.get("common_completed_seed_count") or 0),
        summary=(
            outcome.manifest.get("selected_metric_summary")
            if isinstance(outcome.manifest.get("selected_metric_summary"), dict)
            else outcome.decision_record.get("selected_metric_summary")
        ),
        paired_seed_count=int(outcome.manifest.get("paired_seed_count") or 0),
    )


def _render_guardrail_panel(
    assessment: DecisionGuardrailAssessment | None,
    *,
    title: str,
) -> None:
    st.markdown(f"**{title}**")
    if assessment is None:
        st.info("No guardrail evidence is available yet for this context.")
        return

    verdict_message = f"Guardrail verdict: `{assessment.verdict}` on `{_metric_label(assessment.selected_metric)}`."
    if assessment.verdict == "pass":
        st.success(verdict_message)
    elif assessment.verdict == "weak":
        st.warning(verdict_message)
    else:
        st.error(verdict_message)

    summary_columns = st.columns(6)
    summary_columns[0].metric("Common DOIs", assessment.common_doi_count)
    summary_columns[1].metric("Common Completed", assessment.common_completed_seed_count)
    summary_columns[2].metric("Wins", assessment.wins)
    summary_columns[3].metric("Losses", assessment.losses)
    summary_columns[4].metric("Ties", assessment.ties)
    summary_columns[5].metric("Paired Seeds", assessment.paired_seed_count)

    st.caption(
        "Improvement delta mean / median: "
        f"`{assessment.improvement_delta_mean}` / `{assessment.improvement_delta_median}`"
    )
    st.caption(
        "A `weak` or `fail` verdict means the evidence looks limited, tie-heavy, or otherwise not strong enough "
        "for confident promotion by default. It does not mean the saved files are invalid."
    )
    for reason in assessment.reasons:
        st.markdown(f"- {reason}")


def _render_study_source_context_panel(
    assessment: StudySourceContextAssessment,
) -> None:
    if assessment.source_study_id is None:
        return

    st.markdown("**Study Source Context**")
    if assessment.active:
        st.success(
            "This comparison pair still matches a cohort-study selection, so that study lineage will be included in new comparison saves."
        )
    elif assessment.stale:
        st.warning(assessment.message or "Study-source context is stale and will be ignored for saving.")
    else:
        st.info(assessment.message or "Study-source context is present but inactive.")

    summary_columns = st.columns(5)
    summary_columns[0].metric("Study ID", assessment.source_study_id or "n/a")
    summary_columns[1].metric("Reference Batch", assessment.source_reference_batch_id or "n/a")
    summary_columns[2].metric("Candidate Batch", assessment.source_candidate_batch_id or "n/a")
    summary_columns[3].metric("Decision", assessment.source_candidate_decision or "n/a")
    summary_columns[4].metric("Study Metric", _metric_label(assessment.source_selected_metric))

    detail_columns = st.columns([1.8, 1.0, 1.0])
    with detail_columns[0]:
        st.markdown(f"**Study Directory:** `{assessment.source_study_dir or 'n/a'}`")
    with detail_columns[1]:
        st.markdown(f"**Suggested Decision:** `{assessment.source_suggested_decision or 'n/a'}`")
    with detail_columns[2]:
        if st.button("Clear study source context", use_container_width=True):
            _clear_study_source_context()
            st.rerun()


def _submit_save_reeval_outcome(
    *,
    primary_bundle: BatchUiBundle | None,
    secondary_bundle: BatchUiBundle | None,
    common_doi_count: int,
    common_completed_seed_count: int,
    paired_rows: list[dict[str, Any]],
    summary: Any,
    guardrail_assessment: DecisionGuardrailAssessment | None,
) -> None:
    try:
        request = build_reeval_outcome_save_request(
            candidate_run_dir=str(st.session_state.get("ui_reeval_candidate_run_dir", "")),
            outcome_id=str(st.session_state.get("ui_reeval_outcome_id", "")),
            reviewer=str(st.session_state.get("ui_reeval_reviewer", "")),
            decision_status=str(st.session_state.get("ui_reeval_decision_status", "")),
            notes=str(st.session_state.get("ui_reeval_notes", "")),
            selected_metric=str(st.session_state.get("ui_reeval_metric", "")),
        )
        candidate_run = load_candidate_run_context(request.candidate_run_dir)
        compatibility_errors = candidate_run_compatibility_errors(
            candidate_run=candidate_run,
            primary_bundle=primary_bundle,
            secondary_bundle=secondary_bundle,
        )
        if compatibility_errors:
            raise ReevalOutcomeExportError(compatibility_errors[0])
        if summary is None:
            raise ReevalOutcomeExportError("No usable summary is available for the selected re-eval metric.")
        result = save_reeval_outcome_artifacts(
            request=request,
            candidate_run=candidate_run,
            primary_bundle=primary_bundle,
            secondary_bundle=secondary_bundle,
            common_doi_count=common_doi_count,
            common_completed_seed_count=common_completed_seed_count,
            paired_rows=paired_rows,
            summary=summary,
            guardrail_assessment=guardrail_assessment,
        )
    except ReevalOutcomeExportError as exc:
        st.session_state["ui_reeval_error"] = str(exc)
        st.session_state["ui_reeval_result"] = None
        return

    _clear_baseline_promotion_feedback()
    st.session_state["ui_reeval_error"] = None
    st.session_state["ui_reeval_result"] = result


def _submit_baseline_promotion(
    *,
    primary_bundle: BatchUiBundle | None,
    secondary_bundle: BatchUiBundle | None,
    guardrail_assessment: DecisionGuardrailAssessment | None,
) -> None:
    try:
        request = build_baseline_promotion_request(
            candidate_run_dir=str(st.session_state.get("ui_promotion_candidate_run_dir", "")),
            outcome_dir=str(st.session_state.get("ui_promotion_outcome_dir", "")),
            baseline_id=str(st.session_state.get("ui_promotion_baseline_id", "")),
            reviewer=str(st.session_state.get("ui_promotion_reviewer", "")),
            notes=str(st.session_state.get("ui_promotion_notes", "")),
            override_used=bool(st.session_state.get("ui_promotion_override_used", False)),
            override_reason=str(st.session_state.get("ui_promotion_override_reason", "")),
        )
        candidate_run = load_candidate_run_context(request.candidate_run_dir)
        outcome = load_outcome_context(request.outcome_dir)
        compatibility_errors = promotion_compatibility_errors(
            candidate_run=candidate_run,
            outcome=outcome,
            primary_bundle=primary_bundle,
            secondary_bundle=secondary_bundle,
        )
        if compatibility_errors:
            raise BaselinePromotionError(compatibility_errors[0])
        result = save_accepted_baseline_artifacts(
            base_dir=REPO_ROOT / "runs" / "accepted_baselines",
            request=request,
            candidate_run=candidate_run,
            outcome=outcome,
            guardrail_assessment=guardrail_assessment,
            primary_bundle=primary_bundle,
            secondary_bundle=secondary_bundle,
        )
    except (BaselinePromotionError, ReevalOutcomeExportError) as exc:
        st.session_state["ui_promotion_error"] = str(exc)
        st.session_state["ui_promotion_result"] = None
        return

    st.session_state["ui_promotion_error"] = None
    st.session_state["ui_promotion_result"] = result


def _clear_review_packet_feedback(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    state["ui_review_packet_error"] = None
    state["ui_review_packet_result"] = None


def _clear_reply_preview_feedback(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    state["ui_reply_preview_error"] = None
    state["ui_reply_preview_result"] = None


def _clear_candidate_apply_feedback(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    state["ui_candidate_apply_error"] = None
    state["ui_candidate_apply_warning"] = None
    state["ui_candidate_apply_result"] = None
    state["ui_candidate_apply_in_progress"] = False


def _clear_reeval_outcome_feedback(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    state["ui_reeval_error"] = None
    state["ui_reeval_result"] = None


def _clear_baseline_promotion_feedback(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    state["ui_promotion_error"] = None
    state["ui_promotion_result"] = None
    state["ui_promotion_override_used"] = False
    state["ui_promotion_override_reason"] = ""


def _clear_study_source_context(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    state["ui_study_source_context"] = None


def _set_study_source_context_for_comparison(
    study_entry: CohortStudyRegistryEntry,
    candidate_row: CohortStudyCandidateRow,
) -> None:
    try:
        st.session_state["ui_study_source_context"] = build_study_source_session_payload(
            study_id=study_entry.study_id,
            study_dir=study_entry.study_dir,
            reference_batch_id=study_entry.reference_batch_id or "",
            candidate_batch_id=candidate_row.candidate_batch_id,
            candidate_decision=candidate_row.decision,
            suggested_decision=candidate_row.suggested_decision,
            selected_metric=candidate_row.selected_metric or study_entry.selected_metric,
        )
    except ValueError:
        _clear_study_source_context()


def _get_current_study_source_assessment(
    primary_bundle: BatchUiBundle | None,
    secondary_bundle: BatchUiBundle | None,
) -> StudySourceContextAssessment:
    return evaluate_study_source_context(
        st.session_state.get("ui_study_source_context"),
        primary_batch_id=(
            _optional_str(getattr(getattr(primary_bundle, "manifest", None), "batch_id", None))
            if primary_bundle is not None
            else None
        ),
        secondary_batch_id=(
            _optional_str(getattr(getattr(secondary_bundle, "manifest", None), "batch_id", None))
            if secondary_bundle is not None
            else None
        ),
    )


def _clear_inbox_feedback(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    state["ui_inbox_feedback"] = None
    state["ui_inbox_error"] = None


def _set_inbox_feedback(message: str) -> None:
    st.session_state["ui_inbox_feedback"] = message
    st.session_state["ui_inbox_error"] = None


def _set_inbox_error(message: str) -> None:
    st.session_state["ui_inbox_error"] = message
    st.session_state["ui_inbox_feedback"] = None


def _set_study_source_context_from_inbox_item(item: WorkspaceInboxItem) -> None:
    try:
        st.session_state["ui_study_source_context"] = build_study_source_session_payload(
            study_id=item.study_id or "",
            study_dir=item.artifact_dir,
            reference_batch_id=item.target_primary_batch_id or "",
            candidate_batch_id=item.target_secondary_batch_id or "",
            candidate_decision=item.source_candidate_decision,
            suggested_decision=item.suggested_decision,
            selected_metric=item.selected_metric,
        )
    except ValueError:
        _clear_study_source_context()


def _load_inbox_primary_batch(
    item: WorkspaceInboxItem,
    *,
    preserve_study_source: bool,
) -> str | None:
    if item.target_primary_batch_dir is None or not item.target_primary_batch_exists:
        return "The referenced primary batch is not available on disk."
    try:
        bundle = load_batch_bundle(item.target_primary_batch_dir)
    except BatchLoadError as exc:
        return str(exc)
    _apply_loaded_batch(bundle)
    st.session_state["ui_batch_load_error"] = None
    if preserve_study_source:
        _set_study_source_context_from_inbox_item(item)
    else:
        _clear_study_source_context()
    return None


def _load_inbox_secondary_batch(
    item: WorkspaceInboxItem,
    *,
    preserve_study_source: bool,
) -> str | None:
    if item.target_secondary_batch_dir is None or not item.target_secondary_batch_exists:
        return "The referenced secondary batch is not available on disk."
    try:
        bundle = load_batch_bundle(item.target_secondary_batch_dir)
    except BatchLoadError as exc:
        return str(exc)
    _apply_secondary_comparison_bundle(bundle)
    if preserve_study_source:
        _set_study_source_context_from_inbox_item(item)
    else:
        _clear_study_source_context()
    return None


def _load_review_packet_into_candidate_reply(
    packet_dir: Path,
) -> str | None:
    try:
        bundle = load_review_packet_bundle(packet_dir)
    except ReviewPacketLoadError as exc:
        return str(exc)
    st.session_state["ui_reply_packet_dir_input"] = _display_path(packet_dir)
    st.session_state["ui_loaded_reply_packet"] = bundle
    st.session_state["ui_reply_packet_error"] = None
    _clear_reply_preview_feedback()
    _clear_candidate_apply_feedback()
    return None


def _prepare_candidate_run_for_reeval(
    item: WorkspaceInboxItem,
) -> str | None:
    st.session_state["ui_reeval_candidate_run_dir"] = _display_path(item.artifact_dir)
    _clear_reeval_outcome_feedback()
    _clear_study_source_context()

    warnings: list[str] = []
    primary_error = _load_inbox_primary_batch(item, preserve_study_source=False)
    if primary_error is not None:
        warnings.append(primary_error)
    secondary_error = _load_inbox_secondary_batch(item, preserve_study_source=False)
    if secondary_error is not None:
        warnings.append(secondary_error)
    if warnings:
        return "Candidate run was loaded for outcome work, but the related comparison pair could not be fully restored: " + "; ".join(
            warnings
        )
    return None


def _prepare_outcome_for_promotion(item: WorkspaceInboxItem) -> str | None:
    candidate_run_dir = None
    if isinstance(item.raw_payload, dict):
        candidate_run_dir = _resolve_path_like(item.raw_payload.get("candidate_run_dir"))
    if candidate_run_dir is None:
        return "This saved outcome does not expose a candidate_run_dir for promotion prep."

    st.session_state["ui_promotion_candidate_run_dir"] = _display_path(candidate_run_dir)
    st.session_state["ui_promotion_outcome_dir"] = _display_path(item.artifact_dir)
    _clear_baseline_promotion_feedback()
    _clear_study_source_context()

    warnings: list[str] = []
    primary_error = _load_inbox_primary_batch(item, preserve_study_source=False)
    if primary_error is not None:
        warnings.append(primary_error)
    secondary_error = _load_inbox_secondary_batch(item, preserve_study_source=False)
    if secondary_error is not None:
        warnings.append(secondary_error)
    if warnings:
        return "Promotion context was prepared, but the related comparison pair could not be fully restored: " + "; ".join(
            warnings
        )
    return None


def _load_inbox_pair_for_inspection(item: WorkspaceInboxItem) -> str | None:
    warnings: list[str] = []
    primary_error = _load_inbox_primary_batch(item, preserve_study_source=False)
    if primary_error is not None:
        warnings.append(primary_error)
    secondary_error = _load_inbox_secondary_batch(item, preserve_study_source=False)
    if secondary_error is not None:
        warnings.append(secondary_error)
    if warnings:
        return "; ".join(warnings)
    return None


def _store_loaded_batch_state(
    bundle: BatchUiBundle,
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    state["ui_loaded_batch"] = bundle
    state[BATCH_DIR_STATE_KEY] = _display_path(bundle.batch_dir)
    state[BATCH_DIR_WIDGET_SYNC_PENDING_KEY] = True
    _clear_comparison_save_feedback(state)
    _clear_review_packet_feedback(state)
    _clear_reply_preview_feedback(state)
    _clear_candidate_apply_feedback(state)
    _clear_reeval_outcome_feedback(state)
    _clear_baseline_promotion_feedback(state)
    if bundle.seed_table_rows:
        state["ui_selected_seed_index"] = bundle.seed_table_rows[0]["batch_index"]


def _apply_loaded_batch(bundle: BatchUiBundle) -> None:
    _store_loaded_batch_state(bundle)


def _render_no_loaded_batch_message() -> None:
    st.info("Load an existing batch directory or run a new batch to view artifacts.")
    available_batch_dirs = discover_batch_dirs()
    if available_batch_dirs:
        st.write("Detected batch directories:")
        st.code("\n".join(_display_path(path) for path in available_batch_dirs), language="text")


def _seed_option_label(seed_row: dict[str, Any]) -> str:
    return (
        f"{seed_row.get('batch_index', '?')}: "
        f"{seed_row.get('doi', 'unknown')} "
        f"[{seed_row.get('status', 'unknown')}]"
    )


def _seed_detail_subset_label(subset: str) -> str:
    labels = {
        "all": "All",
        "completed": "Completed",
        "failed": "Failed",
        "best": "Best",
        "worst": "Worst",
    }
    return labels.get(subset, subset.title())


def _seed_detail_doi_label(rows: list[dict[str, Any]], doi: str) -> str:
    seed_row = find_seed_row_by_doi(rows, doi) or {}
    return _seed_option_label(seed_row)


def _sync_diagnostics_metric_state(available_metrics: list[str]) -> None:
    if not available_metrics:
        st.session_state["ui_diag_primary_metric"] = ""
        st.session_state["ui_diag_x_metric"] = ""
        st.session_state["ui_diag_y_metric"] = ""
        return

    if st.session_state.get("ui_diag_primary_metric") not in available_metrics:
        st.session_state["ui_diag_primary_metric"] = choose_default_primary_metric(available_metrics) or ""

    default_x_metric, default_y_metric = choose_default_scatter_metrics(available_metrics)
    if st.session_state.get("ui_diag_x_metric") not in available_metrics:
        st.session_state["ui_diag_x_metric"] = default_x_metric or available_metrics[0]

    current_x_metric = st.session_state.get("ui_diag_x_metric")
    current_y_metric = st.session_state.get("ui_diag_y_metric")
    if current_y_metric not in available_metrics or (
        current_y_metric == current_x_metric and len(available_metrics) > 1
    ):
        if default_y_metric is not None:
            st.session_state["ui_diag_y_metric"] = default_y_metric
        else:
            st.session_state["ui_diag_y_metric"] = current_x_metric


def _sync_comparison_metric_state(primary_ranking_metric: str | None, common_metrics: list[str]) -> None:
    if not common_metrics:
        st.session_state["ui_comparison_metric"] = ""
        return

    if st.session_state.get("ui_comparison_metric") not in common_metrics:
        st.session_state["ui_comparison_metric"] = (
            choose_default_comparison_metric(primary_ranking_metric, common_metrics) or common_metrics[0]
        )


def _sync_review_packet_metric_state(default_metric: str, common_metrics: list[str]) -> None:
    if not common_metrics:
        st.session_state["ui_review_packet_metric"] = ""
        return

    current_metric = st.session_state.get("ui_review_packet_metric")
    if current_metric not in common_metrics:
        st.session_state["ui_review_packet_metric"] = (
            default_metric if default_metric in common_metrics else common_metrics[0]
        )


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _select_accepted_baseline_for_run_batch(entry: AcceptedBaselineEntry) -> None:
    accepted_theory_snapshot_path = entry.accepted_theory_snapshot_path
    st.session_state["ui_selected_accepted_baseline_id"] = entry.baseline_id
    st.session_state["ui_selected_accepted_baseline_dir"] = _display_path(entry.baseline_dir)
    st.session_state["ui_selected_accepted_baseline_theory_path"] = (
        _display_path(accepted_theory_snapshot_path) if accepted_theory_snapshot_path is not None else ""
    )
    st.session_state[THEORY_CONFIG_STATE_KEY] = (
        _display_path(accepted_theory_snapshot_path) if accepted_theory_snapshot_path is not None else ""
    )
    st.session_state[THEORY_CONFIG_WIDGET_SYNC_PENDING_KEY] = True


def _select_benchmark_preset_for_run_batch(entry: BenchmarkPresetEntry) -> None:
    values, warnings = build_benchmark_run_batch_values(entry)
    seeds_csv_path = values.get("seeds_csv_path", "")
    st.session_state["ui_selected_benchmark_preset_id"] = entry.preset_id
    st.session_state["ui_selected_benchmark_preset_path"] = _display_path(entry.preset_path)
    st.session_state["ui_selected_benchmark_preset_seeds_csv"] = str(seeds_csv_path)
    st.session_state["ui_selected_benchmark_preset_warning"] = warnings[0] if warnings else None
    st.session_state["ui_run_prefill_payload"] = {
        "seeds_csv_path": seeds_csv_path,
    }


def _select_evaluation_preset_for_run_batch(entry: EvaluationPresetEntry) -> None:
    values, warnings = build_evaluation_run_batch_values(
        entry,
        allowed_label_sources=SUPPORTED_LABEL_SOURCES,
        fallback_label_source=SUPPORTED_LABEL_SOURCES[0],
    )
    st.session_state["ui_selected_eval_preset_id"] = entry.preset_id
    st.session_state["ui_selected_eval_preset_path"] = _display_path(entry.preset_path)
    st.session_state["ui_selected_eval_preset_values"] = dict(values)
    st.session_state["ui_selected_eval_preset_warning"] = warnings[0] if warnings else None
    st.session_state["ui_run_prefill_payload"] = dict(values)


def _select_launch_profile_for_run_batch(
    entry: LaunchProfileEntry,
    *,
    values: dict[str, Any] | None = None,
    warnings: list[str] | None = None,
) -> None:
    selected_values = dict(values) if isinstance(values, dict) else {}
    selected_warnings = list(warnings) if warnings else []
    st.session_state["ui_selected_launch_profile_id"] = entry.profile_id
    st.session_state["ui_selected_launch_profile_path"] = _display_path(entry.profile_path)
    st.session_state["ui_selected_launch_profile_values"] = dict(selected_values)
    st.session_state["ui_selected_launch_profile_warning"] = (
        selected_warnings[0] if selected_warnings else None
    )
    st.session_state["ui_run_prefill_payload"] = dict(selected_values)


def _timeline_entry_label(entry: TimelineEntry) -> str:
    return (
        f"{entry.timestamp_display} | {entry.artifact_type} | "
        f"{entry.artifact_id} | {entry.summary}"
    )


def _experiment_cohort_label(
    cohort_by_key: dict[str, Any],
    cohort_key: str,
) -> str:
    cohort = cohort_by_key.get(cohort_key)
    if cohort is None:
        return cohort_key
    prefix = "Comparable" if getattr(cohort, "comparable", False) else "Unclassified"
    return f"{prefix} | {cohort.summary} | n={cohort.size}"


def _experiment_batch_label(
    row: ExperimentBatchRow | None,
    leaderboard_metric: str | None,
) -> str:
    if row is None:
        return "n/a"
    metric_value = row.metric_means.get(leaderboard_metric) if leaderboard_metric else None
    metric_text = (
        f"{_metric_label(leaderboard_metric)} mean={metric_value}"
        if leaderboard_metric and metric_value is not None
        else f"{_metric_label(leaderboard_metric)} mean=n/a" if leaderboard_metric
        else "metric=n/a"
    )
    return f"{row.batch_id} | {row.status or 'n/a'} | {metric_text}"


def _cohort_reference_batch_label(
    row: ExperimentBatchRow | None,
) -> str:
    if row is None:
        return "n/a"
    return f"{row.batch_id} | {row.status or 'n/a'} | created={row.created_at_display}"


def _cohort_candidate_row_label(
    row: CohortPairwiseAnalysisRow | None,
) -> str:
    if row is None:
        return "n/a"
    delta_text = (
        f"improvement mean={row.improvement_delta_mean}"
        if row.improvement_delta_mean is not None
        else "improvement mean=n/a"
    )
    return (
        f"{row.candidate_batch_id} | {row.candidate_status or 'n/a'} | "
        f"{row.pairwise_status} | {delta_text}"
    )


def _cohort_study_registry_candidate_label(
    row: CohortStudyCandidateRow | None,
) -> str:
    if row is None:
        return "n/a"
    decision_text = row.decision or "n/a"
    delta_text = (
        f"improvement mean={row.improvement_delta_mean}"
        if row.improvement_delta_mean is not None
        else "improvement mean=n/a"
    )
    return (
        f"{row.candidate_batch_id} | {decision_text} | "
        f"{'usable' if row.usable is True else 'unusable' if row.usable is False else 'unknown'} | "
        f"{delta_text}"
    )


def _clear_selected_accepted_baseline(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    state["ui_selected_accepted_baseline_id"] = ""
    state["ui_selected_accepted_baseline_dir"] = ""
    state["ui_selected_accepted_baseline_theory_path"] = ""


def _clear_selected_benchmark_preset(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    state["ui_selected_benchmark_preset_id"] = ""
    state["ui_selected_benchmark_preset_path"] = ""
    state["ui_selected_benchmark_preset_seeds_csv"] = ""
    state["ui_selected_benchmark_preset_warning"] = None


def _clear_selected_evaluation_preset(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    state["ui_selected_eval_preset_id"] = ""
    state["ui_selected_eval_preset_path"] = ""
    state["ui_selected_eval_preset_values"] = None
    state["ui_selected_eval_preset_warning"] = None


def _clear_selected_launch_profile(
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    state = st.session_state if session_state is None else session_state
    state["ui_selected_launch_profile_id"] = ""
    state["ui_selected_launch_profile_path"] = ""
    state["ui_selected_launch_profile_values"] = None
    state["ui_selected_launch_profile_warning"] = None


def _run_batch_eval_values_match_selected_preset(
    session_state: MutableMapping[str, Any] | None,
    expected_values: dict[str, Any],
) -> bool:
    state = st.session_state if session_state is None else session_state
    comparisons = {
        "max_references": state.get("ui_run_max_references"),
        "max_related": state.get("ui_run_max_related"),
        "max_hard_negatives": state.get("ui_run_max_hard_negatives"),
        "top_k": state.get("ui_run_top_k"),
        "label_source": state.get("ui_run_label_source"),
        "refresh": state.get("ui_run_refresh"),
    }
    return all(comparisons.get(field_name) == expected_values.get(field_name) for field_name in comparisons)


def _run_batch_form_matches_selected_launch_profile(
    session_state: MutableMapping[str, Any] | None,
    expected_values: dict[str, Any],
) -> bool:
    state = st.session_state if session_state is None else session_state
    return (
        _normalized_path_string(_get_selected_theory_config_from_widget(state))
        == _normalized_path_string(expected_values.get("theory_config_path"))
        and _normalized_path_string(state.get("ui_seeds_csv_path"))
        == _normalized_path_string(expected_values.get("seeds_csv_path"))
        and _run_batch_eval_values_match_selected_preset(state, expected_values)
    )


def _normalized_path_string(value: str | Path | None) -> str | None:
    text = _optional_str(value)
    if text is None:
        return None
    text = text.replace("\\", "/")
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = REPO_ROOT / path
    return str(path.resolve())


def _path_exists(value: str | Path | None) -> bool:
    text = _optional_str(value)
    if text is None:
        return False
    text = text.replace("\\", "/")
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path.exists()


def _display_summary_value(value: Any) -> Any:
    return "n/a" if value is None else value


def _batch_option_display_values(options: Any) -> dict[str, Any] | None:
    if options is None:
        return None
    return {
        "max_references": getattr(options, "max_references", None),
        "max_related": getattr(options, "max_related", None),
        "max_hard_negatives": getattr(options, "max_hard_negatives", None),
        "top_k": getattr(options, "top_k", None),
        "label_source": getattr(options, "label_source", None),
        "refresh": getattr(options, "refresh", None),
    }


def _metric_label(metric_name: str | None) -> str:
    if not metric_name:
        return "n/a"
    return METRIC_LABELS.get(metric_name, metric_name)


def _diagnostic_seed_label(seed_row: dict[str, Any]) -> str:
    return f"{seed_row.get('batch_index', '?')}: {seed_row.get('doi', 'unknown')}"


def _yes_no_value(value: object) -> str:
    if value is True:
        return "Yes"
    if value is False:
        return "No"
    return "n/a"


def _seed_detail_text_block(values: dict[str, Any]) -> str:
    return "\n".join(f"{key}: {value}" for key, value in values.items())


def _rows_to_table_data(rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    if not rows:
        return {}

    column_order: list[str] = []
    for row in rows:
        for key in row:
            if key not in column_order:
                column_order.append(key)
    return {
        column_name: [row.get(column_name) for row in rows]
        for column_name in column_order
    }


def _display_path(value: object) -> str:
    if value in (None, ""):
        return ""
    try:
        path = Path(str(value))
    except (TypeError, ValueError):
        return str(value)

    if not path.is_absolute():
        return str(path)
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _resolve_path_like(value: object) -> Path | None:
    text = _optional_str(value)
    if text is None:
        return None
    candidate = Path(text).expanduser()
    if candidate.is_absolute():
        return candidate
    return (REPO_ROOT / candidate).resolve()


if __name__ == "__main__":
    main()
