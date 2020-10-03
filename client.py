import json, logging, os, sys, traceback
from requests import Session
import logger as log

from urllib.parse import urljoin

from threading import Event
from collections import deque
from executor import ThreadPoolExecutor

from phxsocket import Socket
from auth import auth_key, auth_pass, TimeoutException
from worker import Job
from segments import SegmentStore

from versions import get_version
from encode import aom_vpx_encode

import ratelimit


class Client:
  def __init__(self, target, key, name, max_workers, queue_size, threads,
               paths, alt_dl_server):
    self.target = target
    self.ssl = False
    self.first_start = True

    self.key = key
    self.name = name

    self.session = Session()
    self.upload_queue = ThreadPoolExecutor(1)

    self.queue_size = queue_size
    self.segment_store = SegmentStore(self)
    self.workers = ThreadPoolExecutor(max_workers)

    self.socket = None
    self.socket_id = None

    self.hit = 0
    self.miss = 0

    self.paths = paths

    self.encode = {
      "aomenc":
      lambda job: aom_vpx_encode("aom", threads, paths["ffmpeg"], paths[
        "aomenc"], job),
      "vpxenc":
      lambda job: aom_vpx_encode("vpx", threads, paths["ffmpeg"], paths[
        "vpxenc"], job)
    }

    self.versions = {
      "aomenc": get_version("aomenc", paths["aomenc"]),
      "vpxenc": get_version("vpxenc", paths["vpxenc"])
    }

    self.alt_dl_server = alt_dl_server

    self.screen = None

    self.exit_event = Event()

  def stop(self):
    self.segment_store.dispose()
    self.exit_event.set()

  def upload(self, job, output):
    self.upload_queue.submit(self._upload, self._after_upload, job, output)
    self.push_job_state()

  def _upload(self, job, output):
    try:
      with open(output, "rb") as file:
        files = [("file", (output, file, "application/octet"))]
        url = urljoin(self.get_target_url(), "api/finish_segment")
        logging.log(log.Levels.NET,
                    "uploading {} to {}".format(job.segment, url))

        encode_settings = {
          "encoder_params": job.encoder_params,
          "ffmpeg_params": job.ffmpeg_params,
          "passes": job.passes,
          "encoder": job.encoder,
          "version": self.versions[job.encoder]
        }

        params = {
          "segment": job.segment,
          "key": self.key,
          "socket_id": self.socket_id,
          "encode_settings": json.dumps(encode_settings)
        }

        r = self.session.post(url, data=params, files=files, timeout=5)

        j = r.json()
        logging.log(log.Levels.NET, j)

        if j["success"]:
          self.hit += 1
        else:
          self.miss += 1

    except:
      logging.error(traceback.format_exc())
    finally:
      try:
        job.dispose()
      except:
        logging.error(traceback.format_exc())

  def _after_upload(self, resp, job, output):
    try:
      if os.path.exists(output):
        os.remove(output)
    except:
      logging.error(traceback.format_exc())

    try:
      self.push_job_state()
    except:
      logging.error(traceback.format_exc())

  def connect(self):
    while True:
      try:
        self._connect()
        return
      except TimeoutException as e:
        if self.first_start:
          raise e
        logging.log(log.Levels.NET, "timed out, trying again.")

  def _connect(self):
    ssl, token = auth_key(self.target, self.key)

    if ssl:
      self.ssl = True
      logging.log(log.Levels.NET, "using ssl")

    logging.log(log.Levels.NET, "connecting to websocket")

    socket_url = "ws{}://{}/websocket".format("s" if ssl else "", self.target)
    socket = Socket(socket_url, {"token": token})
    self.socket = socket

    socket.on_open = self.on_open
    socket.on_error = self.on_error
    socket.on_close = self.on_close
    socket.connect()

    self.channel = None

  def reconnect(self):
    logging.log(log.Levels.NET, "reconnecting")
    if self.socket:
      self.socket.close()

    self.connect()

  def on_error(self, socket, message):
    logging.log(log.Levels.NET, message)

  def on_open(self, socket):
    logging.log(log.Levels.NET, "websocket opened")

    upload_queue, uploading = self.get_upload_queue()
    job_queue, workers = self.get_job_queue()

    params = {
      "state": {
        "platform": sys.platform,
        "workers": workers,
        "max_workers": self.workers.max_workers,
        "job_queue": job_queue,
        "upload_queue": upload_queue,
        "downloading": self.segment_store.segment,
        "uploading": uploading,
        "queue_size": self.queue_size
      },
      "versions": self.versions
    }

    if self.name is not None:
      params["state"]["name"] = self.name

    if self.socket_id is not None:
      params["id"] = self.socket_id

    self.channel = socket.channel("worker", params)
    self.channel.on("push_segment", self.on_job)
    self.channel.on("cancel", self.on_cancel)

    self.socket_id = self.channel.join()

    self.first_start = False

    self.progress_channel = socket.channel("worker_progress")
    self.progress_channel.join()

    logging.log(log.Levels.NET, "connected to channel")

  def on_close(self, socket):
    self.reconnect()

  def on_job(self, payload):
    logging.log(log.Levels.NET, "received job")
    segment_id = payload["segment_id"]
    job_queue, workers = self.get_job_queue()

    if segment_id in job_queue or \
      segment_id == self.segment_store.segment or \
      segment_id in [job["segment"] for job in workers]:
      self.push_job_state()
      return

    self.channel.push("recv_segment", {"downloading": segment_id})
    self.download(payload["url"], Job(self, payload))

  def on_cancel(self, payload):
    try:
      with self.workers.queue_lock:
        for job in list(self.workers.work_queue.queue):
          if job.args[0].segment in payload["segments"]:
            job.args[0].dispose()

        for job in self.workers.working:
          if job.args[0].segment in payload["segments"]:
            job.args[0].dispose()

        for segment in payload["segments"]:
          if self.segment_store.segment == segment:
            self.segment_store.downloading.dispose()

          self.workers.cancel(
            lambda work_item: work_item.args[0].segment == segment)
    except:
      logging.error(traceback.format_exc())

  def add_worker(self):
    self.workers.max_workers += 1
    self.workers._adjust_thread_count()

  def remove_worker(self):
    self.workers.max_workers -= 1
    self.workers._adjust_thread_count()

  def get_workers(self):
    with self.workers.queue_lock:
      return self._get_workers(self.workers.working)

  def _get_workers(self, working):
    workers = []
    for work_item in self.workers.working:
      job = work_item.args[0]
      worker = {
        "project": job.project,
        "segment": job.segment,
        "progress_num": job.progress[1],
        "progress_den": job.frames,
        "pass": job.progress[0]
      }
      workers.append(worker)
    return workers

  def get_job_queue(self):
    with self.workers.queue_lock:
      worker_queue = [
        job.args[0].segment for job in list(self.workers.work_queue.queue)
      ]
      working = self._get_workers(self.workers.working)

      return worker_queue, working

  def get_upload_queue(self):
    with self.upload_queue.queue_lock:
      upload_queue = [
        job.args[0].segment for job in list(self.upload_queue.work_queue.queue)
      ]
      uploading = [
        work_item.args[0].segment for work_item in self.upload_queue.working
      ]
      return upload_queue, uploading

  def work(self, job):
    try:
      self.push_job_state()
      self.refresh_screen("Workers")
      output = self.encode[job.encoder](job)
      if not job.pipe: return None

      if job.pipe.poll() is None:
        job.pipe.kill()

      return output
    except:
      if job.stopped:
        logging.info("cancelled job")
      else:
        logging.error(traceback.format_exc())
      return None

  def after_work(self, resp, job):
    self.refresh_screen("Workers")
    if resp == None:
      job.dispose()
    else:
      self.upload(job, resp)

  def get_target_url(self):
    return "http{}://{}".format("s" if self.ssl else "", self.target)

  def download(self, url, job):
    url = urljoin(self.get_target_url(), url)
    self.segment_store.acquire(job.filename, url, job)

  def push_worker_progress(self):
    if not self.progress_channel:
      return
    if ratelimit.can_execute("worker_progress", 1):
      try:
        self.progress_channel.push("update_workers",
                                   {"workers": self.get_workers()})
      except:
        logging.error(traceback.format_exc())

  def push_job_state(self):
    upload_queue, uploading = self.get_upload_queue()
    job_queue, workers = self.get_job_queue()

    params = {
      "workers": workers,
      "max_workers": self.workers.max_workers,
      "job_queue": job_queue,
      "upload_queue": upload_queue,
      "downloading": self.segment_store.segment,
      "uploading": uploading
    }

    try:
      self.channel.push("update", params)
    except:
      logging.error(traceback.format_exc())

  def set_screen(self, screen):
    self.screen = screen

  def refresh_screen(self, tab):
    if self.screen:
      self.screen.refresh_tab(tab)


if __name__ == "__main__":
  logger = log.Logger()
  logger.setup()

  from config import load_config

  config = load_config()

  paths = {
    "aomenc": config.aomenc,
    "vpxenc": config.vpxenc,
    "ffmpeg": config.ffmpeg
  }

  client = Client(
    config.target,
    config.key,
    config.name,
    int(config.workers),
    int(config.queue),
    config.threads,
    paths,
    config.alt_dl_server,
  )

  client.connect()
  client.socket.after_connect()

  import screen, curses

  scr = screen.Screen(client)
  scr.add_tab(screen.WorkerTab(scr, client))
  scr.add_tab(screen.LogTab(scr, logger))

  client.set_screen(scr)

  logging.info("ready")

  scr.attach()
