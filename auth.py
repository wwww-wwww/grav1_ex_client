import logging
from requests import Session
from requests.exceptions import SSLError
from urllib.parse import urljoin
from logger import Levels

session = Session()


class AuthException(Exception):
  pass


class TimeoutException(Exception):
  pass


def auth_key(target, key):
  return auth(target, {"key": key})


def auth_pass(target, username, password):
  return auth(target, {"username": username, "password": "password"})


def auth(target, payload, ssl=True):
  server = f"http{'s' if ssl else ''}://{target}/"
  auth_url = urljoin(server, "api/auth")
  logging.log(Levels.NET, "authenticating with", auth_url)

  try:
    r = session.post(auth_url, json=payload, timeout=5)
  except SSLError:
    return auth(target, payload, False)
  except Exception as e:
    raise TimeoutException()

  resp = r.json()
  if "success" in resp:
    if resp["success"]:
      return ssl, resp["token"]
    else:
      raise AuthException(resp["reason"])
  else:
    raise AuthException("bad protocol")
