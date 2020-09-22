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
from agents import JobQueue, SegmentStore, WorkerStore

from versions import get_version
from encode import aom_vpx_encode

import ratelimit

class Client:
  def __init__(self, target, key, name, max_workers, queue_size, paths):
    self.target = target
    self.ssl = False
    self.first_start = True

    self.key = key
    self.name = name

    self.session = Session()
    self.upload_queue = ThreadPoolExecutor(1)
    
    self.job_queue = JobQueue(self, queue_size)
    self.segment_store = SegmentStore(self)

    self.socket = None
    self.socket_id = None

    self.workers = WorkerStore(self, max_workers)

    self.hit = 0
    self.miss = 0

    self.paths = paths

    self.encode = {
      "aomenc": lambda worker, job: aom_vpx_encode("aom", paths["ffmpeg"], paths["aomenc"], worker, job),
      "vpxenc": lambda worker, job: aom_vpx_encode("vpx", paths["ffmpeg"], paths["vpxenc"], worker, job)
    }

    self.versions = {
      "aomenc": get_version("aomenc", paths["aomenc"]),
      "vpxenc": get_version("vpxenc", paths["vpxenc"])
    }

    self.screen = None

    self.exit_event = Event()

  def stop(self):
    self.workers.stop()
    self.job_queue.stop()
    self.exit_event.set()

  def upload(self, job, output):
    self.upload_queue.submit(self._upload, self._after_upload, job, output)
    self.push_job_state()

  def _upload(self, job, output):
    try:
      with open(output, "rb") as file:
        files = [("file", (output, file, "application/octet"))]
        url = urljoin(self.get_target_url(), "api/finish_segment")
        logging.log(log.Levels.NET, f"uploading {job.segment} to {url}")
        r = self.session.post(url,
          data={
            "segment": job.segment,
            "key": self.key,
            "socket_id": self.socket_id,
            "encode_settings": json.dumps({
              "encoder_params": job.encoder_params,
              "ffmpeg_params": job.ffmpeg_params,
              "passes": job.passes,
              "encoder": job.encoder,
              "version": self.versions[job.encoder]
            })
          },
          files=files, timeout=5)

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
        os.remove(output)
      except:
        logging.error(traceback.format_exc())

  def _after_upload(self, job, output):
    self.push_job_state()

  def connect(self):
    while True:
      try:
        self._connect()
        return
      except TimeoutException as e:
        if self.first_start: raise e
        logging.log(log.Levels.NET, "timed out, trying again.")

  def _connect(self):
    ssl, token = auth_key(self.target, self.key)

    if ssl:
      self.ssl = True
      logging.log(log.Levels.NET, "using ssl")

    logging.log(log.Levels.NET, "connecting to websocket")

    socket_url = f"ws{'s' if ssl else ''}://{self.target}/websocket"
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

    uploading = None
    if len(self.upload_queue.working) > 0:
      uploading = self.upload_queue.working[0].args[0].segment

    params = {
      "state": {
        "platform": sys.platform,
        "workers": self.workers.to_list(),
        "max_workers": self.workers.max_workers,
        "job_queue": self.get_job_queue(),
        "upload_queue": self.get_upload_queue(),
        "downloading": self.segment_store.downloading,
        "uploading": uploading,
        "queue_size": self.job_queue.queue_size
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
    if segment_id in self.get_job_queue() or segment_id == self.segment_store.downloading or segment_id in [worker.job.segment for worker in self.workers.workers if worker.job]:
      return
    self.segment_store.downloading = segment_id
    self.channel.push("recv_segment", {"downloading": segment_id})
    self.download(payload["url"], Job(self, payload))

  def on_cancel(self, payload):
    for segment in payload["segments"]:
      self.job_queue.cancel(segment)
      self.workers.cancel(segment)

  def get_job_queue(self):
    with self.job_queue.ret_lock:
      return [job.segment for job in self.job_queue.queue]

  def get_upload_queue(self):
    return [job.args[0].segment for job in list(self.upload_queue.work_queue.queue)]

  def get_target_url(self):
    return f"http{'s' if self.ssl else ''}://{self.target}"

  def download(self, url, job):
    url = urljoin(self.get_target_url(), url)
    self.segment_store.acquire(job.filename, url, job)

  def push_worker_progress(self):
    if not self.progress_channel: return
    if ratelimit.can_execute("worker_progress", 1):
      params = {
        "workers": self.workers.to_list()
      }
      self.progress_channel.push("update_workers", params)

  def push_job_state(self):
    uploading = None
    if len(self.upload_queue.working) > 0:
      uploading = self.upload_queue.working[0].args[0].segment

    params = {
      "workers": self.workers.to_list(),
      "job_queue": self.get_job_queue(),
      "upload_queue": self.get_upload_queue(),
      "downloading": self.segment_store.downloading,
      "uploading": uploading
    }

    self.channel.push("update", params)

  def set_screen(self, screen):
    self.screen = screen

  def refresh_screen(self, tab):
    if self.screen:
      self.screen.refresh_tab(tab)

if __name__ == "__main__":
  logger = log.Logger()
  logger.setup()

  import argparse

  parser = argparse.ArgumentParser()

  parser.add_argument("target", type=str, nargs="?", default="localhost:4000")
  parser.add_argument("--key", type=str, required=True, help="API key")
  parser.add_argument("--workers", dest="workers", default=1)
  parser.add_argument("--threads", dest="threads", default=8)
  parser.add_argument("--queue", default=3)
  parser.add_argument("--name", default=None, help="Name of the client")
  parser.add_argument("--aomenc", default="aomenc", help="Path to aomenc (default: aomenc)")
  parser.add_argument("--vpxenc", default="vpxenc", help="Path to vpxenc (default: vpxenc)")
  parser.add_argument("--ffmpeg", default="ffmpeg", help="Path to ffmpeg (default: ffmpeg)")

  args = parser.parse_args()

  paths = {
    "aomenc": args.aomenc,
    "vpxenc": args.vpxenc,
    "ffmpeg": args.ffmpeg
  }

  client = Client(args.target, args.key, args.name, int(args.workers), int(args.queue), paths)

  for i in range(0, int(args.workers)):
    client.workers.add_worker()

  client.connect()
  client.socket.after_connect()

  import screen, curses

  scr = screen.Screen(client)
  scr.add_tab(screen.WorkerTab(scr, client))
  scr.add_tab(screen.LogTab(scr, logger))

  client.set_screen(scr)

  logging.info("ready")

  scr.attach()
