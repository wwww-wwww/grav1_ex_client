import os, logging, json, traceback
from util import synchronized
import logger as log

from concurrent.futures import ThreadPoolExecutor
from threading import Condition, Lock
from collections import deque

from worker import Worker

class JobQueue:
  def __init__(self, client, queue_size):
    self.client = client

    self.queue_size = queue_size
    self.queue = deque()
    self.queue_not_empty = Condition(Lock())
    self.queue_lock = Lock()

    self.ret_lock = Lock()

  def stop(self):
    with self.queue_not_empty:
      self.queue_not_empty.notify_all()

    for job in self.queue:
      job.dispose()

  def cancel(self, segment):
    with self.ret_lock:
      for e in self.queue:
        if e.segment == segment:
          self.queue.remove(e)
          e.dispose()

  def push(self, segment):
    with self.queue_not_empty:
      with self.ret_lock:
        self.queue.append(segment)
      self.queue_not_empty.notify()
  
  def pop(self, worker):
    with self.queue_lock:
      with self.queue_not_empty:
        while len(self.queue) == 0:
          self.queue_not_empty.wait()
          if worker.stopped: return None

        with self.ret_lock:
          return self.queue.popleft()

class SegmentStore:
  def __init__(self, client):
    self.client = client

    self.files = {}
    self.lock = Lock()
    self.downloading = None
    self.download_executor = ThreadPoolExecutor(max_workers=1)
    self.download_progress = 0

    self.stopping = False

  def download(self, url, job):
    try:
      r = self.client.session.get(url, timeout=5)
      with open(job.filename, "wb+") as file:
        downloaded = 0
        total_size = int(r.headers["content-length"])
        self.download_progress = 0
        for chunk in r.iter_content(chunk_size=2**16):
          if self.stopping:
            if os.path.exists(job.filename):
              os.remove(job.filename)
            return
          if chunk:
            downloaded += len(chunk)
            self.client.refresh_screen("Workers")
            self.download_progress = downloaded / total_size
            file.write(chunk)
      
      logging.log(log.Levels.NET, "finished downloading", job.filename)
      self.downloading = None
      self.client.job_queue.push(job)
    except:
      self.downloading = None
      job.dispose()
      logging.error(traceback.format_exc())
      if os.path.exists(job.filename):
        os.remove(job.filename)
    finally:
      self.client.push_job_state()

  @synchronized
  def acquire(self, filename, url, job):
    if filename in self.files:
      self.files[filename] += 1
      self.downloading = None
      self.client.job_queue.push(job)
      self.client.push_job_state()
    else:
      self.files[filename] = 1
      logging.log(log.Levels.NET, "downloading", url)
      self.download_executor.submit(self.download, url, job)

  @synchronized
  def release(self, filename):
    if filename in self.files:
      if self.files[filename] == 1:
        del self.files[filename]
        try:
          os.remove(filename)
        except:
          pass
      else:
        self.files[filename] -= 1

class WorkerStore:
  def __init__(self, client, max_workers):
    self.client = client

    self.max_workers = max_workers
    self.workers = []

    self.lock = Lock()

    self.stopped = False
  
  @synchronized
  def to_list(self):
    return [worker.as_dict() for worker in self.workers]

  @synchronized
  def stop(self):
    self.stopped = True
    
    for worker in self.workers:
      worker.kill()

  @synchronized
  def size(self):
    return len(self.workers)

  @synchronized
  def cancel(self, segment):
    for worker in self.workers:
      if worker.job != None and worker.job.segment == segment:
        worker.cancel()

  @synchronized
  def remove(self, worker):
    if len(self.workers) > self.max_workers or self.stopped or worker.stopped:
      self.remove_worker(worker)
      return True
    else:
      return False

  @synchronized
  def add_worker(self):
    if self.stopped: return
    self.workers.append(Worker(self.client))

  @synchronized
  def remove_worker(self, worker):
    if worker in self.workers:
      self.workers.remove(worker)
      self.client.refresh_screen("Workers")
