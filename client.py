import json, logging, os
import logger as log

from urllib.parse import urljoin

from threading import Condition, Event, Lock
from collections import deque
from executor import ThreadPoolExecutor

from phxsocket import Socket
from auth import auth_key, auth_pass, TimeoutException
from worker import Worker, Job

class Client:
  def __init__(self, args):
    self.target = args.target
    self.ssl = False

    self.key = args.key
    self.name = args.name

    self.upload_queue = ThreadPoolExecutor(1)
    
    self.job_queue_size = int(args.queue)
    self.job_queue = deque()
    self.job_queue_lock = Lock()
    self.job_queue_not_empty = Condition(self.job_queue_lock)
    self.job_queue_ret_lock = Lock()
    
    self.downloading = None

    self.socket = None
    self.socket_id = None

    self.max_workers = args.workers
    self.workers = []

    self.exit_event = Event()

  def connect(self, first_time=False):
    try:
      ssl, token = auth_key(self.target, self.key)
    except TimeoutException as e:
      if first_time: raise e
      logging.log(log.Levels.NET, "timed out, trying again.")
      self.connect()
      return

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
        "max_workers": self.max_workers,
        "job_queue": self.get_job_queue(),
        "upload_queue": self.get_upload_queue(),
        "downloading": self.downloading,
        "uploading": uploading,
        "queue_size": self.job_queue_size
      }
    }

    if self.name is not None:
      params["name"] = self.name

    if self.socket_id is not None:
      params["id"] = self.socket_id

    self.channel = socket.channel("worker", params)
    self.channel.on("push_job", self.on_job)
    self.socket_id = self.channel.join()

    logging.log(log.Levels.NET, "connected to channel")

  def on_close(self, socket):
    self.reconnect()

  def on_job(self, payload):
    logging.log(log.Levels.NET, payload)
    self.channel.push("recv_job", {"downloading": payload["segment_id"]})
    self.download(payload["url"], Job(payload))

  def get_job_queue(self):
    return [job.segment for job in self.job_queue]

  def get_upload_queue(self):
    return [job.args[0].segment for job in list(self.upload_queue.work_queue.queue)]

  def download(self, url, job):
    url = urljoin(f"http{'s' if self.ssl else ''}://{self.target}", url)
    logging.log(log.Levels.NET, "downloading", url)

if __name__ == "__main__":
  logger = log.Logger()
  logger.setup()

  args = type("", (), {})
  args.target = "192.168.1.50:4000"
  args.key = "TJdaaoLTTFCz8AOu+/Ca0SflwksArRHj"
  args.name = None
  args.queue = 0
  args.workers = 3

  client = Client(args)

  #for i in range(0, int(args.workers)):
  #  client.add_worker(Worker(client))

  client.connect(True)
  client.socket.after_connect()

  import screen, curses

  scr = screen.Screen(client.exit_event)
  scr.add_tab(screen.WorkerTab(scr, client.workers))
  scr.add_tab(screen.LogTab(scr, logger))

  logging.info("ready")

  scr.attach()
