import logging, traceback
from requests import Session, RequestException
from requests.exceptions import SSLError
from urllib.parse import urljoin
from logger import Levels

session = Session()


class AuthError(Exception):
  pass


class TimeoutError(Exception):
  pass


def auth_key(target, key):
  return auth(target, {"key": key})


def auth_pass(target, username, password):
  return auth(target, {"username": username, "password": "password"})


def auth(target, payload, ssl=True):
  server = "http{}://{}/".format("s" if ssl else "", target)
  auth_url = urljoin(server, "api/auth")
  logging.log(Levels.NET, "authenticating with", auth_url)

  try:
    r = session.post(auth_url, json=payload, timeout=5)
  except SSLError:
    return auth(target, payload, False)
  except RequestException:
    raise TimeoutError()

  try:
    resp = r.json()
    if resp["success"]:
      return ssl, resp["token"]
    else:
      raise AuthError(resp["reason"])
  except AuthError as e:
    raise e
  except:
    raise AuthError("bad protocol", traceback.format_exc())
