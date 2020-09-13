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
    self.filename = params["split_name"]

  def dispose(self):
    self.client.segment_store.release(self.filename)

class Worker:
  def __init__(self, client):
    self.client = client
    self.job = None
    
    self.status = ""
    self.pipe = None
    self.stopped = False
    self.progress = (0, 0)
    self.id = 0

    Thread(target=lambda: self.work(), daemon=True).start()

  def as_dict(self):
    return {
      "segment": self.job.segment if self.job else None,
      "progress": self.progress
    }

  def kill(self):
    self.stopped = True

    if self.pipe and self.pipe.poll() is None:
      self.pipe.kill()
    
    if self.job:
      self.job.dispose()

  def update_status(self, *argv):
    message = " ".join([str(arg) for arg in argv])
    self.status = message
    self.client.refresh_screen("Workers")

  def work(self):
    while True:
      self.update_status("waiting")

      if self.client.workers.remove(self): return

      job = self.client.job_queue.pop(self)

      if self.stopped:
        if job:
          job.dispose()

        self.client.workers.remove_worker(self)
        return

      self.job = job

      if not job:
        continue

      self.client.push_job_state()

      try:
        success, output = self.client.encode[self.job.encoder](self, job)
        if self.pipe and self.pipe.poll() is None:
          self.pipe.kill()

        self.pipe = None

        if success:
          job.dispose()
          self.job = None
          self.client.upload(job, output)
        elif output:
          if os.path.exists(output):
            try:
              os.remove(output)
            except: pass
      except:
        logging.error(traceback.format_exc())

    self.client.workers.remove_worker(self)