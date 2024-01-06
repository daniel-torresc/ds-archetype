import datetime
import time
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def timeit(func):
    """
    Decorator that measures the execution time of a function.

    This decorator wraps a function and measures the time it takes to execute the function. It logs the elapsed time
    using the function name.

    Args:
        func: The function to be decorated.

    Returns:
        The result of the decorated function.

    Examples:
        @timeit
        def my_function():
            # Function implementation
            pass
    """

    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()

        logger.info(f"{func.__name__} - Elapsed time {datetime.timedelta(seconds=(end - start))}")
        return result

    return wrapper
