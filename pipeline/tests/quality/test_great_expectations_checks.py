from pipeline.quality.great_expectations_checks import (
    QualityGateError,
    _expectation_configuration,
)


def test_quality_gate_error_retains_structured_failures():
    failures = [
        {
            "table_name": "fct_orders",
            "expectation_type": "ExpectColumnValuesToBeUnique",
            "column": "order_id",
            "observed_result": {"unexpected_count": 2},
        }
    ]

    error = QualityGateError(failures)

    assert error.failures == failures
    assert "fct_orders.ExpectColumnValuesToBeUnique" in str(error)


def test_expectation_configuration_retains_column_and_limits():
    class FakeConfiguration:
        def to_json_dict(self):
            return {"kwargs": {"column": "review_score", "min_value": 1}}

    class FakeExpectation:
        configuration = FakeConfiguration()

    assert _expectation_configuration(FakeExpectation()) == {
        "kwargs": {"column": "review_score", "min_value": 1}
    }
