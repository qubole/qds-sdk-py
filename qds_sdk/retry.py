import time
import logging
from functools import wraps

log = logging.getLogger("retry")


def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, max_delay=32):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = "%s, Retrying in %d seconds..." % (e.__class__.__name__, mdelay)
                    log.info(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
                    if (mdelay >= max_delay):
                        mdelay = max_delay
            return f(*args, **kwargs)
        return f_retry  # true decorator
    return deco_retry
