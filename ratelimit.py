import time

s = {}


def can_execute(key, rate):
  ct = time.time()
  if key in s:
    if ct - s[key] > rate:
      s[key] = ct
    else:
      return False

  else:
    s[key] = ct

  return True
