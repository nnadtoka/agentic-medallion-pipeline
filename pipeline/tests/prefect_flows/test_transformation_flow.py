import json

from pipeline.prefect_flows.transformation_flow import (
    dbt_build_command,
    dbt_failure_details,
)


def test_dbt_build_command_uses_explicit_paths(monkeypatch):
    monkeypatch.setenv("DBT_PROJECT_DIR", "/test/project")
    monkeypatch.setenv("DBT_PROFILES_DIR", "/test/profiles")

    assert dbt_build_command() == [
        "dbt",
        "build",
        "--project-dir",
        "/test/project",
        "--profiles-dir",
        "/test/profiles",
    ]


def test_dbt_build_command_can_request_full_refresh(monkeypatch):
    monkeypatch.setenv("DBT_PROJECT_DIR", "/test/dbt")
    monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)

    assert dbt_build_command(full_refresh=True)[-1] == "--full-refresh"


def test_dbt_build_command_splits_a_single_select_string(monkeypatch):
    monkeypatch.setenv("DBT_PROJECT_DIR", "/test/dbt")
    monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)

    command = dbt_build_command(select="staging intermediate")

    assert command == [
        "dbt",
        "build",
        "--project-dir",
        "/test/dbt",
        "--profiles-dir",
        "/test/dbt",
        "--select",
        "staging",
        "intermediate",
    ]


def test_dbt_build_command_select_and_full_refresh_together(monkeypatch):
    monkeypatch.setenv("DBT_PROJECT_DIR", "/test/dbt")
    monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)

    command = dbt_build_command(select="marts", full_refresh=True)

    assert command[-3:] == ["--select", "marts", "--full-refresh"]
    assert "--vars" not in command


def test_dbt_failure_details_identifies_failed_resources(tmp_path):
    target_directory = tmp_path / "target"
    target_directory.mkdir()
    (target_directory / "run_results.json").write_text(
        json.dumps(
            {
                "metadata": {"invocation_id": "dbt-invocation"},
                "results": [
                    {"unique_id": "model.ok", "status": "success"},
                    {
                        "unique_id": "test.failed_check",
                        "status": "fail",
                        "message": "Got 2 results",
                        "failures": 2,
                        "execution_time": 0.2,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    details = dbt_failure_details(tmp_path, return_code=1)

    assert details["dbt_invocation_id"] == "dbt-invocation"
    assert details["failed_resources"] == [
        {
            "unique_id": "test.failed_check",
            "status": "fail",
            "message": "Got 2 results",
            "failures": 2,
            "execution_time": 0.2,
        }
    ]
