# extremely simplified version of
# https://github.com/python/cpython/blob/master/Lib/concurrent/futures/thread.py
# with accessible work queue, working list, and weighted jobs

import os, threading, itertools, types, logging, traceback
from concurrent.futures import _base
from collections import deque
from contextlib import contextmanager

_shutdown = False
# Lock that ensures that new workers are not created while the interpreter is
# shutting down. Must be held while mutating _threads_queues and _shutdown.
_global_shutdown_lock = threading.Lock()


class _WorkItem(object):
  def __init__(self, weight, fn, args, kwargs, after, after_remove):
    self.weight = weight
    self.fn = fn
    self._after = after
    self._after_remove = after_remove
    self.args = args
    self.kwargs = kwargs
    self.result = None

  def run(self):
    try:
      return self.fn(*self.args, **self.kwargs)
    except:
      logging.error(traceback.format_exc())
      return None

  def after(self, result):
    self.result = result
    try:
      if self._after:
        self._after(result, *self.args, **self.kwargs)
    except:
      logging.error(traceback.format_exc())

  def after_remove(self):
    try:
      if self._after_remove:
        self._after_remove(self.result, *self.args, **self.kwargs)
    except:
      logging.error(traceback.format_exc())


def _worker(tpe):
  while True:
    try:
      with tpe.get_job() as work_item:
        if work_item:
          result = work_item.run()
          work_item.after(result)

    except:
      logging.error(traceback.format_exc())


class ThreadPoolExecutor(_base.Executor):
  _counter = itertools.count().__next__

  def __init__(self, max_workers):
    self.queue_lock = threading.Lock()
    self.work_queue = deque()
    self.working = []
    self._threads = []
    self._shutdown = False
    self._shutdown_lock = threading.Lock()
    self._thread_name_prefix = ("ThreadPoolExecutor-%d" % self._counter())

    self.max_workers = max_workers
    self._value = 0
    self._semaphore = threading.Lock()
    self._cv = threading.Condition()
    self._work_queue_not_empty = threading.Condition()

    self._workers_value = 0
    self._workers_lock = threading.Lock()
    self._workers_available = threading.Condition(self._workers_lock)

    self._shutdown_lock = threading.Lock()

  def workers_acquire(self):
    with self._workers_available:
      while self._workers_value >= self.max_workers:
        self._workers_available.wait()

      self._workers_value += 1

  def workers_release(self):
    with self._workers_available:
      self._workers_value = max(self._workers_value - 1, 0)
      self._workers_available.notify()

  def submit(self,
             fn,
             args=[],
             kwargs={},
             weight=1,
             after=None,
             after_remove=None):
    with self._shutdown_lock, _global_shutdown_lock:
      if self._shutdown:
        raise RuntimeError("cannot schedule new futures after shutdown")

      if _shutdown:
        raise RuntimeError(
          "cannot schedule new futures after interpreter shutdown")

      w = _WorkItem(weight, fn, args, kwargs, after, after_remove)

      with self.queue_lock:
        with self._work_queue_not_empty:
          self.work_queue.append(w)
          self._work_queue_not_empty.notify()

      self._adjust_thread_count()

  def _acquire(self, weight):
    with self._semaphore:
      if self._value + weight <= self.max_workers or self._value == 0:
        self._value += weight
        return True
      else:
        return False

  @contextmanager
  def acquire(self, weight):
    acquired = False
    with self._cv:
      acquired = self._acquire(weight)
      if not acquired:
        self._cv.wait()
      yield acquired

  def _release(self, weight):
    with self._semaphore:
      self._value -= max(weight, 0)
      with self._cv:
        self._cv.notify_all()

  @contextmanager
  def get_job(self):
    with self._work_queue_not_empty:
      while len(self.work_queue) == 0:
        self._work_queue_not_empty.wait()

    with self.queue_lock:
      if len(self.work_queue) == 0:
        yield None
        return

      front = self.work_queue[0]

    self.workers_acquire()

    with self.acquire(front.weight) as acquired:
      if not acquired:
        yield None
        self.workers_release()
        return

      with self.queue_lock:
        if len(self.work_queue) > 0 and self.work_queue[0] == front:
          self.work_queue.popleft()
          self.working.append(front)
        else:
          # V if it was hanging onto an item that has already been removed
          # either from another process or cancel()
          self._release(front.weight)
          yield None
          self.workers_release()
          return

    try:
      yield front
    finally:
      front.after_remove()
      self.workers_release()
      self.working.remove(front)
      self._release(front.weight)
      del front

  def cancel(self, fn):
    new_queue = [
      work_item for work_item in list(self.work_queue) if not fn(work_item)
    ]

    self.work_queue.clear()
    self.work_queue.extend(new_queue)

    with self._cv:
      self._cv.notify_all()

    with self._work_queue_not_empty:
      self._work_queue_not_empty.notify_all()

  def _adjust_thread_count(self):
    with self._workers_available:
      self._workers_available.notify_all()

    num_threads = len(self._threads)
    while num_threads < self.max_workers:
      thread_name = "%s_%d" % (self._thread_name_prefix or self, num_threads)

      t = threading.Thread(
        name=thread_name,
        target=_worker,
        daemon=True,
        args=[self],
      )

      t.start()
      self._threads.append(t)
      num_threads = len(self._threads)

  def shutdown(self, wait=True):
    with self._shutdown_lock:
      self._shutdown = True

      # Send a wake-up to prevent threads calling
      # work_queue.get(block=True) from permanently blocking.
      with self._work_queue_not_empty:
        self._work_queue_not_empty.notify_all()
    if wait:
      for t in self._threads:
        t.join()

  shutdown.__doc__ = _base.Executor.shutdown.__doc__
