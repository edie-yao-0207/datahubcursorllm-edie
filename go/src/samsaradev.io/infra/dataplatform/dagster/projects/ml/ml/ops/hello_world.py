from dagster import OpExecutionContext, op


@op
def hello_world_op(context: OpExecutionContext, start_after=None):
    """Simple hello world op that can be chained after other ops.

    Args:
        start_after: Optional input to create dependency (value is ignored)
    """
    context.log.info("Hello there, World!")
    return "Hello there, World!"


@op
def hello_world2_op(context: OpExecutionContext, start_after=None):
    """Simple hello world op that can be chained after other ops.

    Args:
        start_after: Optional input to create dependency (value is ignored)
    """
    context.log.info("Hello there, World 2!")
    return "Hello there, World 2!"
