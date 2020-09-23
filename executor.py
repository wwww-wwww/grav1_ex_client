# extremely simplified version of
# https://github.com/python/cpython/blob/master/Lib/concurrent/futures/thread.py
# with accessible work queue and working list

import os, threading, itertools, weakref, types, logging, traceback
from concurrent.futures import _base
from queue import Queue

_threads_queues = weakref.WeakKeyDictionary()
_shutdown = False
# Lock that ensures that new workers are not created while the interpreter is
# shutting down. Must be held while mutating _threads_queues and _shutdown.
_global_shutdown_lock = threading.Lock()

class _WorkItem(object):
  def __init__(self, fn, fn2, args, kwargs):
    self.fn = fn
    self.fn2 = fn2
    self.args = args
    self.kwargs = kwargs

  def run(self):
    try:
      return self.fn(*self.args, **self.kwargs)
    except:
      logging.error(traceback.format_exc())
      return None

  def after(self, result):
    try:
      self.fn2(result, *self.args, **self.kwargs)
    except:
      logging.error(traceback.format_exc())

def _worker(executor_reference, queue_lock, work_queue, working):
  try:
    while True:
      work_item = work_queue.get()
 
      if work_item is not None:
        with queue_lock:
          working.append(work_item)

        result = work_item.run()

        with queue_lock:
          working.remove(work_item)

        work_item.after(result)

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
  except:
    logging.error(traceback.format_exc())

class ThreadPoolExecutor(_base.Executor):
  _counter = itertools.count().__next__

  def __init__(self, max_workers):
    if max_workers <= 0:
      raise ValueError("max_workers must be greater than 0")

    self.max_workers = max_workers
    self.queue_lock = threading.Lock()
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

      w = _WorkItem(fn, fn2, args, kwargs)

      with self.queue_lock:
        self.work_queue.put(w)
      self._adjust_thread_count()

  submit.__doc__ = _base.Executor.submit.__doc__

  def cancel(self, fn):
    with self.work_queue.mutex:
      new_queue = [work_item for work_item in list(self.work_queue.queue) if not fn(work_item)]
      
      self.work_queue.queue.clear()

      for work_item in new_queue:
        self.work_queue.put(work_item)

  def _adjust_thread_count(self):
    # if idle threads are available, don"t spin new threads
    if self._idle_semaphore.acquire(timeout=0):
      return

    # When the executor gets lost, the weakref callback will wake up
    # the worker threads.
    def weakref_cb(_, q=self.work_queue):
      q.put(None)

    num_threads = len(self._threads)
    if num_threads < self.max_workers:
      thread_name = "%s_%d" % (self._thread_name_prefix or self, num_threads)

      t = threading.Thread(
        name=thread_name,
        target=_worker,
        daemon=True,
        args=(
          weakref.ref(self, weakref_cb),
          self.queue_lock,
          self.work_queue,
          self.working
        )
      )

      t.start()
      self._threads.add(t)
      _threads_queues[t] = self.work_queue

  def shutdown(self, wait=True):
    with self._shutdown_lock:
      self._shutdown = True

      # Send a wake-up to prevent threads calling
      # work_queue.get(block=True) from permanently blocking.
      self.work_queue.put(None)
    if wait:
      for t in self._threads:
        t.join()

  shutdown.__doc__ = _base.Executor.shutdown.__doc__
