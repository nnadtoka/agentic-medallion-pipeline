from pipeline.transformation_control.batches import generate_batch_id


def test_generate_batch_id_uses_pipeline_prefix_and_posix_seconds():
    assert (
        generate_batch_id(
            pipeline_name="olist_transform_quality",
            unix_seconds=1789320000,
        )
        == "olist_transform_quality_1789320000"
    )
