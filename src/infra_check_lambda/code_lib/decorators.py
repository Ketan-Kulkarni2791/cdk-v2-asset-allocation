"""Decorator functions to be used by the code."""
import logging
import time
from functools import wraps
from typing import Any

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def log_methods_non_sensitive(method: Any) -> Any:
    """Log methods that was called whose parameters have no sensitive info.

    Args:
        method (Any): The function this method decorates.

    Returns:
        Any: The result of the function.
    """
    argnames = method.__code__.co_varnames[:method.__code__.co_argcount]
    fname = method.__name__

    @wraps(method)
    def wrap(*args, **kw):
        start_time = time.time()
        result = method(*args, **kw)
        end_time = time.time()

        function_name = f'{fname}('
        function_arguments = ', '.join(
            '% s = % r' % entry for entry in zip(argnames, args[:len(argnames)])
        )
        variable_length_arguments = f'args = {list(args[:len(argnames):])}'
        variable_length_keywords = f'kwargs = {kw}'
        function_call = f'{function_name}{function_arguments}{variable_length_arguments}' \
                        f'{variable_length_keywords})'
        log_entry = 'function: %r Time Run: %2.2f ms' % (
            function_call, (end_time - start_time) * 1000
        )
        LOGGER.info(log_entry)
        return result

    return wrap


def log_method_sensitive(method: Any) -> Any:
    """Log method that was called whose parameter have sensitive info.

    Args:
        method (Any): The function this decorates.

    Returns:
        Any: The result of the function.
    """

    fname = method.__name__

    @wraps(method)
    def wrap(*args, **kw):
        start_time = time.time()
        result = method(*args, **kw)
        end_time = time.time()

        function_name = f'{fname}('
        function_call = f'{function_name}'
        log_entry = 'function: %r Time Run: %2.2f ms' % (
            function_call, (end_time - start_time) * 1000
        )
        LOGGER.info(log_entry)
        return result

    return wrap        
        
        
        