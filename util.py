from wrapt import decorator
from urllib.parse import urljoin as _urljoin


@decorator
def synchronized(wrapped, instance, args, kwargs):
  with instance.lock:
    return wrapped(*args, **kwargs)


def print_progress(n, total):
  fill = "█" * int((n / total) * 10)
  return "{:3.0f}%|{:{}s}| {}/{}".format(100 * n / total, fill, 10, n, total)


def urljoin(*args):
  if len(args) == 1: return args
  url = _urljoin(str(args[0]) + "/", str(args[1]))
  if len(args) == 2: return url
  return urljoin(url, *args[2:])


class JobStopped(Exception):
  pass


class EncodingError(Exception):
  pass
