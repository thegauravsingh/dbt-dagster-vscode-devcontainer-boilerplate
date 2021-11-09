from dagster_dbt import DbtCliOutput
from typing import Any, Dict, Iterator, Optional
from dagster import AssetMaterialization, EventMetadataEntry
import dateutil


def generate_materializations_dbt_test(
    dbt_output: DbtCliOutput,
) -> Iterator[AssetMaterialization]:
    for test in dbt_output.result["results"]:
        materialization = result_to_materialization_dbt_test(test)
        if materialization is not None:
            yield materialization


def result_to_materialization_dbt_test(
    test_result: Dict[str, Any]
) -> Optional[AssetMaterialization]:
    metadata = []

    for timing in test_result["timing"]:
        if timing["name"] == "execute":
            desc = "Execution"
        else:
            continue

        started_at = dateutil.parser.isoparse(timing["started_at"])
        completed_at = dateutil.parser.isoparse(timing["completed_at"])
        duration = completed_at - started_at
        metadata.extend(
            [
                EventMetadataEntry.text(
                    text=started_at.isoformat(timespec="seconds"),
                    label=f"{desc} Started At",
                ),
                EventMetadataEntry.text(
                    text=started_at.isoformat(timespec="seconds"),
                    label=f"{desc} Completed At",
                ),
                EventMetadataEntry.float(
                    value=duration.total_seconds(), label=f"{desc} Duration"
                ),
            ]
        )

    metadata.append(
        EventMetadataEntry.text(text=test_result["status"], label=f"Status")
    )

    metadata.append(
        EventMetadataEntry.int(value=test_result["failures"], label=f"Failures")
    )

    unique_id = test_result["unique_id"]

    return AssetMaterialization(
        description=f"dbt node: {unique_id}",
        metadata_entries=metadata,
        asset_key=unique_id.split(".")[:-1],
    )
