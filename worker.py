import logging

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

  def kill(self):
    self.stopped = True

    if self.pipe and self.pipe.poll() is None:
      self.pipe.kill()
    
    if self.job:
      self.job.dispose()

  def _update_status(self, *argv):
    message = " ".join([str(arg) for arg in argv])
    self.status = message

  def work(self):
    while True:
      self._update_status("waiting")

      if self.client.workers.remove(self): return

      self.job = self.client.job_queue.pop()

      if self.stopped:
        if self.job:
          self.job.dispose()

        self.client.workers.remove_worker(self)
        return

      if not self.job:
        continue

      try:
        success, output = self.client.encode[self.job.encoder](self, self.job)
        if self.pipe and self.pipe.poll() is None:
          self.pipe.kill()

        self.pipe = None

        if success:
          self.client.upload(self.job, output)
          self.job.dispose()
          self.job = None
        elif output:
          if os.path.exists(output):
            try:
              os.remove(output)
            except: pass
      except: pass

    self.client.workers.remove_worker(self)