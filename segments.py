import os, logging, json, traceback
from util import synchronized
import logger as log

from concurrent.futures import ThreadPoolExecutor
from threading import Condition, Lock
from collections import deque


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
          if self.stopping or job.stopped:
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
      self.client.workers.submit(self.client.work, self.client.after_work, job)
    except:
      logging.error(traceback.format_exc())
      self.downloading = None
      job.dispose()
    finally:
      self.client.push_job_state()

  @property
  def segment(self):
    return self.downloading.segment if self.downloading else None

  @synchronized
  def acquire(self, filename, url, job):
    if filename in self.files:
      self.files[filename] += 1
      self.downloading = None
      self.client.workers.submit(self.client.work, self.client.after_work, job)
    else:
      self.files[filename] = 1
      self.downloading = job
      logging.log(log.Levels.NET, "downloading", url)
      self.download_executor.submit(self.download, url, job)

    self.client.push_job_state()

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
