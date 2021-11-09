from pipeline.dbt_test_utils import generate_materializations_dbt_test
from dagster.config.config_type import Noneable
from dagster_dbt import dbt_cli_resource, DbtCliOutput
from dagster_dbt.utils import generate_materializations
from datetime import datetime
from dagster import (
    pipeline,
    solid,
    ModeDefinition,
    repository,
    Output,
    AssetKey,
    asset_sensor,
    RunRequest,
    schedule,
    PresetDefinition,
)



my_dbt_resource = dbt_cli_resource.configured(
    {
        "project_dir": "/workspace/dbt_demo",
        "dbt_executable": "/usr/local/bin/dbt",
    }
)


def hour_filter(_context):
    hour = datetime.now().hour
    return hour >= 6 or hour == 0


@solid(required_resource_keys={"dbt"})
def dbt_seed(context) -> DbtCliOutput:
    dbt_output = context.resources.dbt.seed()
    return dbt_output


@solid(
    required_resource_keys={"dbt"},
    config_schema={"models": Noneable(str), "exclude": Noneable(str)},
)
def run_dbt_models(context, run_result: DbtCliOutput) -> DbtCliOutput:
    dbt_output = context.resources.dbt.run(
        context.solid_config["models"], context.solid_config["exclude"]
    )
    for materialization in generate_materializations(dbt_output):
        yield materialization
    yield Output(dbt_output)


@solid(
    required_resource_keys={"dbt"},
    config_schema={"models": Noneable(str), "exclude": Noneable(str)},
)
def test_dbt_models(context, run_result: DbtCliOutput) -> DbtCliOutput:
    dbt_output = context.resources.dbt.test(
        context.solid_config["models"], context.solid_config["exclude"]
    )
    for materialization in generate_materializations_dbt_test(dbt_output):
        yield materialization
    yield Output(dbt_output)


@solid(
    required_resource_keys={"dbt"},
    config_schema={"select": Noneable(str), "exclude": Noneable(str)},
)
def dbt_snapshot(context) -> DbtCliOutput:
    dbt_output = context.resources.dbt.snapshot(
        context.solid_config["select"], context.solid_config["exclude"]
    )
    yield Output(dbt_output)


# TODO: Load the presets from yaml files using PresetDefinition.from_files
# https://docs.dagster.io/tutorial/advanced-tutorial/pipelines#pipeline-config-presets
@pipeline(
    mode_defs=[ModeDefinition(resource_defs={"dbt": my_dbt_resource})],
    tags={"use-case": "dbt_run"},
    preset_defs=[
        PresetDefinition(
            "daily_schedule",
            run_config={
                "solids": {
                    "run_dbt_models": {"config": {"exclude": "tag:monthly+"}},
                    "test_dbt_models": {"config": {"exclude": "tag:monthly+"}},
                }
            },
        ),
        PresetDefinition(
            "monthly_schedule",
            run_config={
                "solids": {
                    "run_dbt_models": {"config": {"models": "tag:monthly+"}},
                    "test_dbt_models": {"config": {"models": "tag:monthly+"}},
                }
            },
        ),
    ],
)
def dbt_run_and_test_pipeline():
    dbt_seed_result = dbt_seed()
    run_result = run_dbt_models(dbt_seed_result)
    test_dbt_models(run_result)


@pipeline(mode_defs=[ModeDefinition(resource_defs={"dbt": my_dbt_resource})])
def dbt_snapshot_and_run_downstream_pipeline():
    snapshot_result = dbt_snapshot()
    run_dbt_models(snapshot_result)


@asset_sensor(
    asset_key=AssetKey(["model", "dbt_demo", "stg_customers"]),
    pipeline_name="dbt_snapshot_and_run_downstream_pipeline",
)
def asset_sensor_customer_snapshot(context, asset_event):
    snapshot_name = "customer_status_snapshot"
    yield RunRequest(
        run_key=context.cursor,
        run_config={
            "solids": {
                "dbt_snapshot": {"config": {"select": snapshot_name}},
                "run_dbt_models": {"config": {"models": f"{snapshot_name}+"}},
            }
        },
    )


# TODO: Use modes instead of retyping the presets from the pipeline
@schedule(
    # 8am every day
    cron_schedule="0 8 * * *",
    pipeline_name="dbt_run_and_test_pipeline",
    execution_timezone="Australia/Sydney",
    should_execute=hour_filter,
)
def dbt_daily_schedule(date):
    exclude_models = "tag:monthly+"
    return {
        "solids": {
            "run_dbt_models": {"config": {"exclude": exclude_models}},
            "test_dbt_models": {"config": {"exclude": exclude_models}},
        }
    }


@schedule(
    # 10am on the first day of each month
    cron_schedule="0 10 1 * *",
    pipeline_name="dbt_run_and_test_pipeline",
    execution_timezone="Australia/Sydney",
)
def dbt_monthly_schedule(date):
    run_models = "tag:monthly+"
    return {
        "solids": {
            "run_dbt_models": {"config": {"models": run_models}},
            "test_dbt_models": {"config": {"models": run_models}},
        }
    }


@repository
def dbt_repository():
    return [
        dbt_run_and_test_pipeline,
        dbt_snapshot_and_run_downstream_pipeline,
        asset_sensor_customer_snapshot,
        dbt_daily_schedule,
        dbt_monthly_schedule,
    ]
