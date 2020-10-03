import os, logging, json, traceback
from util import synchronized, urljoin
import logger as log

from concurrent.futures import ThreadPoolExecutor
from threading import Condition, Lock
from collections import deque


class DownloadCancelled(Exception):
  pass


class SegmentStore:
  def __init__(self, client):
    self.client = client

    self.files = {}
    self.lock = Lock()
    self.downloading = None
    self.download_executor = ThreadPoolExecutor(max_workers=1)
    self.download_progress = 0

    self.jobs = []

    self.stopping = False

  def dispose(self):
    self.stopping = True
    for job in self.jobs:
      job.dispose()

  def download(self, url, job):
    try:
      self.download_progress = 0
      self.client.refresh_screen("Workers")
      if self.client.alt_dl_server is not None:
        ext_url = urljoin(self.client.alt_dl_server, job.project, job.file)
        r = self.client.session.get(ext_url, timeout=5)

        if r.status_code != 200:
          r = self.client.session.get(url, timeout=5)
        else:
          logging.log(log.Levels.NET, "downloading from",
                      self.client.alt_dl_server)
      else:
        r = self.client.session.get(url, timeout=5)

      if r.status_code != 200:
        raise DownloadCancelled()

      with open(job.filename, "wb+") as file:
        downloaded = 0
        total_size = int(r.headers["content-length"])
        for chunk in r.iter_content(chunk_size=2**16):
          if self.stopping or job.stopped:
            raise DownloadCancelled()

          if chunk:
            downloaded += len(chunk)
            self.client.refresh_screen("Workers")
            self.download_progress = downloaded / total_size
            file.write(chunk)

      logging.log(log.Levels.NET, "finished downloading", job.filename)
      self.client.workers.submit(self.client.work, self.client.after_work, job)
    except:
      logging.error(traceback.format_exc())
      job.dispose()

    self.downloading = None
    try:
      self.client.push_job_state()
    except:
      logging.error(traceback.format_exc())

  @property
  def segment(self):
    return self.downloading.segment if self.downloading else None

  @synchronized
  def acquire(self, filename, url, job):
    self.jobs.append(job)

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
  def release(self, job):
    self.jobs.remove(job)
    if job.filename in self.files:
      if self.files[job.filename] == 1:
        del self.files[job.filename]
        try:
          os.remove(job.filename)
        except:
          pass
      else:
        self.files[job.filename] -= 1
