import logging
from enum import IntEnum


class Levels(IntEnum):
  NET = 21


class LogMessage:
  def __init__(self, record, msg):
    self.record = record
    self.msg = msg


class Logger(logging.Handler):
  def __init__(self):
    super(Logger, self).__init__()
    self.messages = []
    self.on_message = None

  def format(self, record):
    if type(record.msg) is tuple:
      record.msg = " ".join([str(s) for s in record.msg])
    msg = [str(record.msg)] + [str(s) for s in record.args]
    record.msg = " ".join(msg)
    return f"[{record.levelname.lower()}] {record.msg}"

  def emit(self, record):
    formatted = self.format(record)
    msg = LogMessage(record, formatted)

    self.messages.append(msg)
    self.messages = self.messages[-1000:]

    if self.on_message:
      self.on_message(msg)
    else:
      print(formatted)

  def setup(self):
    for name, e in Levels.__members__.items():
      logging.addLevelName(e.value, name)

    root = logging.getLogger()
    root.addHandler(self)
    root.setLevel(20)
