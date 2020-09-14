# extremely simplified version of
# https://github.com/python/cpython/blob/master/Lib/concurrent/futures/thread.py
# with accessible work queue and working list

import os, threading, itertools, weakref, types
from concurrent.futures import _base
from queue import Queue

_threads_queues = weakref.WeakKeyDictionary()
_shutdown = False
# Lock that ensures that new workers are not created while the interpreter is
# shutting down. Must be held while mutating _threads_queues and _shutdown.
_global_shutdown_lock = threading.Lock()

class _WorkItem(object):
  def __init__(self, future, fn, fn2, args, kwargs):
    self.future = future
    self.fn = fn
    self.fn2 = fn2
    self.args = args
    self.kwargs = kwargs

  def run(self):
    if not self.future.set_running_or_notify_cancel():
      return

    try:
      result = self.fn(*self.args, **self.kwargs)
    except BaseException as exc:
      self.future.set_exception(exc)
      # Break a reference cycle with the exception 'exc'
      self = None
    else:
      self.future.set_result(result)

  def after(self):
    self.fn2(*self.args, **self.kwargs)

def _worker(executor_reference, work_queue, working):
  try:
    while True:
      work_item = work_queue.get(block=True)
      if work_item is not None:
        working.append(work_item)
        work_item.run()
        working.remove(work_item)
        work_item.after()

        # Delete references to object. See issue16284
        del work_item

        # attempt to increment idle count
        executor = executor_reference()
        if executor is not None:
          executor._idle_semaphore.release()

        del executor
        continue

      executor = executor_reference()
      # Exit if:
      #   - The interpreter is shutting down OR
      #   - The executor that owns the worker has been collected OR
      #   - The executor that owns the worker has been shutdown.
      if _shutdown or executor is None or executor._shutdown:
        # Flag the executor as shutting down as early as possible if it
        # is not gc-ed yet.
        if executor is not None:
          executor._shutdown = True
        # Notice other workers
        work_queue.put(None)
        return
      del executor
  except BaseException:
    _base.LOGGER.critical('Exception in worker', exc_info=True)

class ThreadPoolExecutor(_base.Executor):
  _counter = itertools.count().__next__

  def __init__(self, max_workers=None):
    if max_workers is None:
      max_workers = min(32, (os.cpu_count() or 1) + 4)

    if max_workers <= 0:
      raise ValueError("max_workers must be greater than 0")

    self._max_workers = max_workers
    self.work_queue = Queue()
    self.working = []
    self._idle_semaphore = threading.Semaphore(0)
    self._threads = set()
    self._shutdown = False
    self._shutdown_lock = threading.Lock()
    self._thread_name_prefix = ("ThreadPoolExecutor-%d" % self._counter())

  def submit(self, fn, fn2, /, *args, **kwargs):
    with self._shutdown_lock, _global_shutdown_lock:

      if self._shutdown:
        raise RuntimeError("cannot schedule new futures after shutdown")

      if _shutdown:
        raise RuntimeError("cannot schedule new futures after interpreter shutdown")

      f = _base.Future()
      w = _WorkItem(f, fn, fn2, args, kwargs)

      self.work_queue.put(w)
      self._adjust_thread_count()
      return f

  submit.__doc__ = _base.Executor.submit.__doc__

  def _adjust_thread_count(self):
    # if idle threads are available, don"t spin new threads
    if self._idle_semaphore.acquire(timeout=0):
      return

    # When the executor gets lost, the weakref callback will wake up
    # the worker threads.
    def weakref_cb(_, q=self.work_queue):
      q.put(None)

    num_threads = len(self._threads)
    if num_threads < self._max_workers:
      thread_name = "%s_%d" % (self._thread_name_prefix or self, num_threads)

      t = threading.Thread(
        name=thread_name,
        target=_worker,
        daemon=True,
        args=(
          weakref.ref(self, weakref_cb),
          self.work_queue,
          self.working
        )
      )

      t.start()
      self._threads.add(t)
      _threads_queues[t] = self.work_queue

  def shutdown(self, wait=True, *, cancel_futures=False):
    with self._shutdown_lock:
      self._shutdown = True
      if cancel_futures:
        # Drain all work items from the queue, and then cancel their
        # associated futures.
        while True:
          try:
            work_item = self.work_queue.get_nowait()
          except queue.Empty:
            break
          if work_item is not None:
            work_item.future.cancel()

      # Send a wake-up to prevent threads calling
      # work_queue.get(block=True) from permanently blocking.
      self.work_queue.put(None)
    if wait:
      for t in self._threads:
        t.join()

  shutdown.__doc__ = _base.Executor.shutdown.__doc__
