from wrapt import decorator

@decorator
def synchronized(wrapped, instance, args, kwargs):
  with instance.lock:
    return wrapped(*args, **kwargs)
