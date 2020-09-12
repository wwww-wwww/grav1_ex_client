import os, logging, json
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
    self.queue_lock = Lock()
    self.queue_not_empty = Condition(self.queue_lock)
    self.queue_ret_lock = Lock()

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
    r = self.session.get(url)
    try:
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
    except:
      if os.path.exists(job.filename):
        os.remove(job.filename)

  @synchronized
  def acquire(self, filename, url, job):
    if filename in self.files:
      self.files[filename] += 1
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
