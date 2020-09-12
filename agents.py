import os, logging, json, traceback
import logger as log

from requests import Session
from concurrent.futures import ThreadPoolExecutor
from threading import Condition, Lock
from collections import deque

from wrapt import decorator

class JobQueue:
  def __init__(self, client, queue_size):
    self.client = client

    self.queue_size = queue_size
    self.queue = deque()
    self.queue_not_empty = Condition(Lock())
    self.queue_lock = Lock()

  def push(self, segment):
    with self.queue_not_empty:
      self.queue.append(segment)
      self.queue_not_empty.notify()
  
  def pop(self):
    if self.queue_size > 0:
      with self.queue_lock:
        with self.queue_not_empty:
          while len(self.queue) == 0:
            self.queue_not_empty.wait()
            if worker.stopped: return None
          
          return self.queue.popleft()

@decorator
def synchronized(wrapped, instance, args, kwargs):
  with instance.lock:
    return wrapped(*args, **kwargs)

class SegmentStore:
  def __init__(self, client):
    self.client = client

    self.session = Session()

    self.files = {}
    self.lock = Lock()
    self.download_executor = ThreadPoolExecutor(max_workers=1)

    self.stopping = False

  def download(self, url, job):
    try:
      r = self.session.get(url)
      with open(job.filename, "wb+") as file:
        downloaded = 0
        total_size = int(r.headers["content-length"])
        for chunk in r.iter_content(chunk_size=2**16):
          if self.stopping:
            if os.path.exists(job.filename):
              os.remove(job.filename)
            return
          if chunk:
            downloaded += len(chunk)
            file.write(chunk)
      
      logging.log(log.Levels.NET, "finished downloading", job.filename)
      self.client.downloading = None
      self.client.job_queue.push(job)
      with self.client.job_queue.queue_lock:
        self.client.push_job_state()
    except:
      logging.error(traceback.format_exc())
      if os.path.exists(job.filename):
        os.remove(job.filename)

  @synchronized
  def acquire(self, filename, url, job):
    if filename in self.files:
      self.files[filename] += 1
      self.client.downloading = None
      self.client.job_queue.push(job)
      with self.client.job_queue.queue_lock:
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
        os.remove(filename)
      else:
        self.files[filename] -= 1
