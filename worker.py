class Job:
  def __init__(self, segment):
    self.segment = segment

class Worker:
  def __init__(self, client):
    self.client = client
    self.job = None
