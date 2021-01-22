import json, logging, os, sys, shutil, traceback, phxsocket, time
from requests import Session
import logger as log

from urllib.parse import urljoin

from threading import Event, Lock
from collections import deque
from executor import ThreadPoolExecutor

from auth import auth_key, auth_pass, TimeoutError
from segments import Job, SegmentStore

from encode import get_encoders, get_versions

import ratelimit, updater


class Client:
  def __init__(self, config, paths):
    self.config = config
    self.ssl = False
    self.first_start = True

    self.session = Session()
    self.upload_queue = ThreadPoolExecutor(1)

    self.segment_store = SegmentStore(self)
    self.workers = ThreadPoolExecutor(config.workers)

    self.state_lock = Lock()

    self.socket = None
    self.socket_id = None
    self.uuid = None

    self.socket_open = Event()

    self.hit = 0
    self.miss = 0

    self.paths = paths
    self.encoders = get_encoders(paths)
    self.encode = lambda job: self.encoders[job.encoder].encode(
      job, self.paths, config.threads)

    self.screen = None

    self.exit_event = Event()
    self.exit_exc = None

  def stop(self):
    self.segment_store.dispose()
    self.upload_queue.shutdown()
    self.exit_event.set()

  def upload(self, job, output):
    self.upload_queue.submit(
      self._upload,
      [job, output],
      after_remove=self._after_upload,
    )
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
          "version": self.encoders[job.encoder].version
        }

        params = {
          "segment": job.segment,
          "key": self.config.key,
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
    finally:
      self.refresh_screen("Workers")
      self.push_job_state()

  def connect(self, fail_after=False):
    while True:
      try:
        return self._connect()
      except phxsocket.channel.ChannelConnectError as e:
        if fail_after:
          raise e

        if "reason" not in e.args[0]:
          raise e

        if e.args[0]["reason"] not in ["bad versions", "missing encoders"]:
          raise e

        encoders = e.args[0]["data"]
        logging.error(e.args[0]["reason"], encoders)

        if updater.update_encoders(self.get_target_url(), encoders):
          for enc in encoders:
            if shutil.which("./{}".format(enc)):
              self.paths[enc] = shutil.which("./{}".format(enc))
            else:
              self.paths[enc] = enc

            self.encoders[enc].get_version(self.paths)

          with self.workers.queue_lock:
            for job in self.workers.working:
              job.args[0].dispose()

          return self.reconnect(True)
        else:
          logging.info("Unable to download binaries from target server")
          raise Exception("Unable to update")

      except TimeoutError as e:
        if self.first_start:
          raise e
        logging.log(log.Levels.NET, "timed out, trying again.")
      except Exception as e:
        if self.first_start:
          raise e
        else:
          self.exit_exc = e
          self.stop()

      return

  def _connect(self):
    ssl, token = auth_key(self.config.target, self.config.key)

    self.ssl = ssl
    if ssl:
      logging.log(log.Levels.NET, "using ssl")

    logging.log(log.Levels.NET, "connecting to websocket")

    socket_url = "ws{}://{}/websocket".format("s" if ssl else "",
                                              self.config.target)
    if self.socket:
      self.socket.set_params({"token": token}, url=socket_url)
    else:
      self.socket = phxsocket.Client(socket_url, {"token": token})

      self.socket.on_open = self.on_open
      self.socket.on_error = self.on_error
      self.socket.on_close = self.on_close
      self.socket.on_message = lambda msg: logging.info(msg)

    self.channel = None

    return self.socket.connect()

  def reconnect(self, fail_after=False):
    logging.log(log.Levels.NET, "reconnecting")
    try:
      self.socket.close()
    except phxsocket.client.SocketClosedError:
      pass

    return self.connect(fail_after)

  def on_error(self, socket, message):
    logging.log(log.Levels.NET, message)

  def on_open(self, socket):
    logging.log(log.Levels.NET, "websocket opened")

    upload_queue, uploading = self.get_upload_queue()
    job_queue, workers, weight = self.get_job_queue()

    params = {
      "meta": {
        "platform": sys.platform,
      },
      "state": {
        "workers": workers,
        "max_workers": self.workers.max_workers,
        "job_queue": job_queue,
        "upload_queue": upload_queue,
        "downloading": self.segment_store.segment,
        "uploading": uploading,
        "queue_size": self.config.queue,
        "weighted_workers": weight
      },
      "versions": get_versions(self.encoders)
    }

    if self.config.name:
      params["meta"]["name"] = self.config.name

    if self.socket_id and self.uuid:
      params["id"] = self.socket_id
      params["uuid"] = self.uuid

    self.channel = socket.channel("worker", params)
    self.channel.on("push_segment", self.on_job)
    self.channel.on("cancel", self.on_cancel)
    self.channel.on("set_workers", self.on_set_workers)

    resp = self.channel.join()
    logging.info(resp)

    self.socket_id = resp["sock_id"]
    self.uuid = resp["uuid"]

    self.first_start = False

    self.progress_channel = socket.channel("worker_progress")
    self.progress_channel.join()

    logging.log(log.Levels.NET, "connected to channel")
    self.socket_open.set()

  def on_close(self, socket):
    if self.first_start: return
    logging.log(log.Levels.NET, "closed")
    self.socket_open.clear()
    while not self.reconnect():
      logging.log(log.Levels.NET, "Failed to reconnect, retrying in 5 seconds")
      time.sleep(5)

  def on_job(self, payload):
    with self.state_lock:
      segment_id = payload["segment_id"]
      job_queue, workers, _weight = self.get_job_queue()

      if segment_id in job_queue or \
        self.segment_store.downloading or \
        segment_id in [job["segment"] for job in workers]:
        self._push_job_state()
        return

      logging.log(log.Levels.NET, "received job", str(payload))
      while True:
        try:
          self.channel.push("recv_segment", {"downloading": segment_id})
          break
        except phxsocket.client.SocketClosedError:
          self.socket_open.wait()

      self.download(payload["url"], Job(self, payload))
      self._push_job_state()

  def on_cancel(self, payload):
    try:
      with self.workers.queue_lock:
        for job in list(self.workers.work_queue):
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
    finally:
      self.push_job_state()

  def on_set_workers(self, payload):
    self.set_workers(payload["n"])
    self.refresh_screen("Workers")

  def set_workers(self, n):
    self.workers.max_workers = n
    self.workers._adjust_thread_count()
    self.push_job_state()

  def add_worker(self):
    self.set_workers(self.workers.max_workers + 1)

  def remove_worker(self):
    self.set_workers(max(self.workers.max_workers - 1, 0))

  def get_workers(self):
    with self.workers.queue_lock:
      return self._get_workers(self.workers.working)[0]

  def _get_workers(self, working):
    workers = []
    weight = 0
    for work_item in self.workers.working:
      job = work_item.args[0]
      worker = {
        "project": job.project,
        "segment": job.segment,
        "progress_num": job.progress[1],
        "progress_den": job.frames,
        "pass": job.progress[0]
      }
      weight += job.weight
      workers.append(worker)

    return workers, weight

  def get_job_queue(self):
    with self.workers.queue_lock:
      worker_queue = [
        job.args[0].segment for job in list(self.workers.work_queue)
      ]
      working, weight = self._get_workers(self.workers.working)

      return worker_queue, working, weight

  def get_upload_queue(self):
    with self.upload_queue.queue_lock:
      upload_queue = [
        job.args[0].segment for job in list(self.upload_queue.work_queue)
      ]
      uploading = [
        work_item.args[0].segment for work_item in self.upload_queue.working
      ]
      return upload_queue, uploading

  def work(self, job):
    try:
      if job.stopped:
        return None

      self.push_job_state()
      self.refresh_screen("Workers")
      output = self.encode(job)
      if not job.pipe: return None

      return output
    except:
      if job.stopped:
        logging.info("cancelled job")
      else:
        logging.error(traceback.format_exc())
      return None

  def after_work(self, resp, job):
    if resp:
      self.upload(job, resp)
    else:
      job.dispose()

  def after_work_remove(self, resp, job):
    self.refresh_screen("Workers")
    self.push_job_state()

  def get_target_url(self):
    return "http{}://{}".format("s" if self.ssl else "", self.config.target)

  def download(self, url, job):
    url = urljoin(self.get_target_url(), url)
    self.segment_store.acquire(job, url)

  def push_worker_progress(self):
    if not self.progress_channel:
      return

    if ratelimit.can_execute("worker_progress", 1):
      try:
        self.progress_channel.push("update_workers",
                                   {"workers": self.get_workers()})
      except phxsocket.client.SocketClosedError:
        pass
      except:
        logging.error(traceback.format_exc())

  def push_job_state(self):
    with self.state_lock:
      self._push_job_state()

  def _push_job_state(self):
    upload_queue, uploading = self.get_upload_queue()
    job_queue, workers, weight = self.get_job_queue()

    params = {
      "workers": workers,
      "max_workers": self.workers.max_workers,
      "job_queue": job_queue,
      "upload_queue": upload_queue,
      "downloading": self.segment_store.segment,
      "uploading": uploading,
      "weighted_workers": weight
    }

    try:
      if self.channel:
        self.channel.push("update", params)
    except phxsocket.client.SocketClosedError:
      pass
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

  for path in paths:
    if paths[path] in [path, f"./{path}"] and path != "ffmpeg":
      continue
    elif not shutil.which(paths[path]):
      logging.error(path, "not found at", paths[path])
      exit(1)

  client = Client(config, paths)

  client.connect()

  if client.exit_exc:
    raise client.exit_exc

  import screen, curses

  scr = screen.Screen(client)
  scr.add_tab(screen.WorkerTab(scr, client))
  scr.add_tab(screen.LogTab(scr, logger))

  client.set_screen(scr)

  logging.info("ready")

  try:
    scr.attach()
  finally:
    if client.exit_exc:
      raise client.exit_exc
