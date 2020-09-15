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

  def dispose(self):
    self.client.segment_store.release(self.filename)

class Worker:
  def __init__(self, client):
    self.client = client
    self.job = None
    
    self.status = ""
    self.pipe = None
    self.progress = (0, 0)

    self.stopped = False
    self.cancel_job = False

    Thread(target=lambda: self.work(), daemon=True).start()

  def as_dict(self):
    return {
      "segment": self.job.segment if self.job else None,
      "progress_num": self.progress[1],
      "progress_den": self.job.frames if self.job else 0,
      "pass": self.progress[0]
    }

  def kill(self):
    self.stopped = True
    self.cancel_job = True

    if self.pipe and self.pipe.poll() is None:
      self.pipe.kill()

    if self.job:
      self.job.dispose()

  def cancel(self):
    self.cancel_job = True
    if self.pipe and self.pipe.poll() is None:
      self.pipe.kill()

  def update_progress(self):
    self.client.push_worker_progress()

  def update_status(self, *argv):
    message = " ".join([str(arg) for arg in argv])
    self.status = message
    self.client.refresh_screen("Workers")

  def work(self):
    while True:
      self.update_status("waiting")

      if self.client.workers.remove(self): return

      self.job = None
      self.cancel_job = False

      job = self.client.job_queue.pop(self)

      if job and (self.stopped or self.cancel_job):
        job.dispose()
        
      if self.stopped or not job:
        continue

      self.job = job

      self.client.push_job_state()

      try:
        output = self.client.encode[self.job.encoder](self, job)
        if self.pipe and self.pipe.poll() is None:
          self.pipe.kill()

        self.pipe = None

        job.dispose()
        self.job = None

        if self.cancel_job:
          os.remove(output)
        else:
          self.client.upload(job, output)
          
      except:
        job.dispose()
        self.job = None
        self.client.push_job_state()
        logging.error(traceback.format_exc())

    self.client.workers.remove_worker(self)