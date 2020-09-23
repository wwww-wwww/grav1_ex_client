from wrapt import decorator


@decorator
def synchronized(wrapped, instance, args, kwargs):
  with instance.lock:
    return wrapped(*args, **kwargs)


def print_progress(n, total):
  fill = "█" * int((n / total) * 10)
  return "{:3.0f}%|{:{}s}| {}/{}".format(100 * n / total, fill, 10, n, total)
