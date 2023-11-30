import inspect


def _format_keys(data: dict, obj=None):
    return {
        key.replace(" ", "_").replace("-", "_"): value for key, value in data.items()
        if obj is None or key.replace(" ", "_").replace("-", "_") in inspect.signature(obj).parameters
    }
