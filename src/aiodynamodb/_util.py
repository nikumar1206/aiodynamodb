from typing import Any, get_args, get_origin


def _resolve_key_annotation(annotation: Any) -> type:
    """Resolve optional/union annotations to a concrete key type."""
    origin = get_origin(annotation)
    if origin is None:
        return annotation
    args = [arg for arg in get_args(annotation) if arg is not type(None)]
    if len(args) == 1:
        return _resolve_key_annotation(args[0])
    return annotation
