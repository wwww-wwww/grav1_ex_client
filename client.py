import json, logging, os
import logger as log

from urllib.parse import urljoin

from threading import Event
from collections import deque
from executor import ThreadPoolExecutor

from phxsocket import Socket
from auth import auth_key, auth_pass, TimeoutException
from worker import Worker, Job
from agents import JobQueue, SegmentStore, WorkerStore

from encode import aom_vpx_encode

class Client:
  def __init__(self, target, key, name, max_workers, queue_size):
    self.target = target
    self.ssl = False

    self.key = key
    self.name = name

    self.upload_queue = ThreadPoolExecutor(1)
    
    self.job_queue = JobQueue(self, queue_size)
    self.segment_store = SegmentStore(self)

    self.socket = None
    self.socket_id = None

    self.workers = WorkerStore(self, max_workers)

    self.encode = {
      "aomenc": lambda worker, job: aom_vpx_encode("aom", args.aomenc, worker, job),
      "vpxenc": lambda worker, job: aom_vpx_encode("vpx", args.vpxenc, worker, job)
    }

    self.exit_event = Event()

  def connect(self, first_time=False):
    while True:
      try:
        self._connect()
        return
      except TimeoutException as e:
        if first_time: raise e
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
      uploading = self.upload_queue.working[0].segment

    params = {
      "state": {
        "workers": [],
        "max_workers": self.workers.max_workers,
        "job_queue": self.get_job_queue(),
        "upload_queue": self.get_upload_queue(),
        "downloading": self.segment_store.downloading,
        "uploading": uploading,
        "queue_size": self.job_queue.queue_size
      }
    }

    if self.name is not None:
      params["name"] = self.name

    if self.socket_id is not None:
      params["id"] = self.socket_id

    self.channel = socket.channel("worker", params)
    self.channel.on("push_segment", self.on_job)
    self.socket_id = self.channel.join()

    logging.log(log.Levels.NET, "connected to channel")

  def on_close(self, socket):
    self.reconnect()

  def on_job(self, payload):
    logging.log(log.Levels.NET, payload)
    self.segment_store.downloading = payload["segment_id"]
    self.channel.push("recv_segment", {"downloading": payload["segment_id"]})
    self.download(payload["url"], Job(self, payload))

  def get_job_queue(self):
    return [job.segment for job in self.job_queue.queue]

  def get_upload_queue(self):
    return [job.args[0].segment for job in list(self.upload_queue.work_queue.queue)]

  def download(self, url, job):
    url = urljoin(f"http{'s' if self.ssl else ''}://{self.target}", url)
    self.segment_store.acquire(job.filename, url, job)

  def push_job_state(self):
    uploading = None
    if len(self.upload_queue.working) > 0:
      uploading = self.upload_queue.working[0].segment

    params = {
      "workers": [],
      "job_queue": self.get_job_queue(),
      "upload_queue": self.get_upload_queue(),
      "downloading": self.segment_store.downloading,
      "uploading": uploading
    }

    self.channel.push("update", params)

if __name__ == "__main__":
  logger = log.Logger()
  logger.setup()

  target = "192.168.1.50:4000"
  key = "TJdaaoLTTFCz8AOu+/Ca0SflwksArRHj"
  name = None
  workers = 3
  queue_size = 0

  client = Client(target, key, name, workers, queue_size)

  #for i in range(0, int(args.workers)):
  #  client.add_worker(Worker(client))

  client.connect(True)
  client.socket.after_connect()

  import screen, curses

  scr = screen.Screen(client.exit_event)
  scr.add_tab(screen.WorkerTab(scr, client))
  scr.add_tab(screen.LogTab(scr, logger))

  logging.info("ready")

  scr.attach()
