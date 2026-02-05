import pytest

from trakt.core.steps import ResolvedStep, StepBindingError, step_contract


def test_step_contract_sets_declared_bindings_and_capabilities() -> None:
    @step_contract(
        inputs=["input"],
        outputs=["output"],
        supports_batch=False,
        supports_stream=True,
    )
    def run(ctx, input):
        return {"output": input}

    resolved = ResolvedStep.from_definition(
        step_id="normalize",
        uses="steps.normalize.demo",
        handler=run,
        bindings={
            "input": "source__records",
            "output": "records_norm",
        },
    )

    assert resolved.declared_inputs == ["input"]
    assert resolved.declared_outputs == ["output"]
    assert resolved.supports_batch is False
    assert resolved.supports_stream is True


def test_step_contract_rejects_duplicate_names() -> None:
    with pytest.raises(ValueError, match="duplicate name 'input'"):
        @step_contract(inputs=["input", "input"], outputs=["output"])
        def run(ctx, input):
            return {"output": input}


def test_step_binding_error_suggests_closest_binding_name() -> None:
    @step_contract(inputs=["input"], outputs=["output"])
    def run(ctx, input):
        return {"output": input}

    with pytest.raises(StepBindingError, match="did you mean 'input'"):
        ResolvedStep.from_definition(
            step_id="normalize",
            uses="steps.normalize.demo",
            handler=run,
            bindings={
                "inpt": "source__records",
                "output": "records_norm",
            },
        )
