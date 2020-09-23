import logging, traceback, os
from threading import Thread


class Job:
  def __init__(self, client, params):
    self.client = client

    self.segment = params["segment_id"]
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

  def dispose(self):
    if not self.disposed:
      self.disposed = True
      self.client.segment_store.release(self.filename)

  def update_progress(self):
    self.client.push_worker_progress()

  def update_status(self, *argv):
    message = " ".join([str(arg) for arg in argv])
    self.status = message
    self.client.refresh_screen("Workers")

  def kill(self):
    self.stopped = True

    if self.pipe and self.pipe.poll() is None:
      self.pipe.kill()
  
    self.dispose()
