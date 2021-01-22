import os, logging, json, traceback, re
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

    self.stopping = False

  def dispose(self):
    self.stopping = True

    for job in [job for f in self.files for job in self.files[f]]:
      job.dispose()

    self.download_executor.shutdown()

  def save(self, r, job):
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

  def download_alt(self, url, job):
    if not self.client.config.alt_dl_server:
      return False

    ext_url = urljoin(
      self.client.config.alt_dl_server,
      job.project,
      "split",
      job.file,
    )

    with self.client.session.get(ext_url, timeout=5, stream=True) as r:
      if r.status_code != 200:
        return False
      else:
        logging.log(log.Levels.NET, "downloading from", ext_url)
      self.save(r, job)
      return True

  def download(self, url, job):
    logging.log(log.Levels.NET, "downloading", job.filename)

    try:
      self.download_progress = 0
      self.client.refresh_screen("Workers")
      if not self.download_alt(url, job):
        logging.log(log.Levels.NET, "downloading from", url)
        with self.client.session.get(url, timeout=5, stream=True) as r:
          self.save(r, job)

      logging.log(log.Levels.NET, "finished downloading", job.filename)
      with self.client.state_lock:
        self.client.workers.submit(
          self.client.work,
          [job],
          weight=job.weight,
          after=self.client.after_work,
          after_remove=self.client.after_work_remove,
        )
    except:
      if not job.stopped:
        logging.error(traceback.format_exc())
        job.dispose()

    with self.client.state_lock:
      self.downloading = None

    self.client.push_job_state()
    self.client.refresh_screen("Workers")

  @property
  def segment(self):
    return self.downloading.segment if self.downloading else None

  @synchronized
  def acquire(self, job, url):
    if job.filename in self.files:
      self.files[job.filename].append(job)
      self.downloading = None

      self.client.workers.submit(
        self.client.work,
        [job],
        weight=job.weight,
        after=self.client.after_work,
        after_remove=self.client.after_work_remove,
      )
    else:
      self.files[job.filename] = [job]
      self.downloading = job
      self.download_executor.submit(self.download, url, job)

  @synchronized
  def release(self, job):
    if job.filename in self.files:
      if job in self.files[job.filename]:
        self.files[job.filename].remove(job)

    if len(self.files[job.filename]) == 0:
      del self.files[job.filename]
      try:
        os.remove(job.filename)
      except:
        pass


class Job:
  def __init__(self, client, params):
    self.client = client

    self.segment = params["segment_id"]
    self.project = params["project_id"]
    self.file = params["file"]
    self.start = params["start"]
    self.frames = params["frames"]
    self.encoder = params["encoder"]
    self.passes = params["passes"]
    self.encoder_params = params["encoder_params"]
    self.ffmpeg_params = params["ffmpeg_params"]
    self.grain_table = params["grain_table"]
    self.filename = "tmp{}".format(params["split_name"])

    self.status = ""
    self.pipe = None
    self.progress = (0, 0)

    self.stopped = False
    self.disposed = False

    self.weight = self._get_weight()

  def _get_weight(self):
    if self.encoder not in self.client.config.weights:
      return 1

    if self.encoder in ["aomenc", "vpxenc"]:
      for param in self.encoder_params:
        match = re.match("--cpu-used=(.+)", param)
        if match:
          cpu_used = f"cpu{match.group(1)}"
          if cpu_used in self.client.config.weights[self.encoder]:
            return self.client.config.weights[self.encoder][cpu_used]

          break

    return 1

  def dispose(self):
    self.stopped = True

    if self.pipe:
      if self.pipe.poll() is None:
        self.pipe.kill()

    if not self.disposed:
      self.disposed = True
      self.client.segment_store.release(self)

  def update_progress(self):
    self.client.push_worker_progress()

  def update_status(self, *argv):
    message = " ".join([str(arg) for arg in argv])
    self.status = message
    self.client.refresh_screen("Workers")
