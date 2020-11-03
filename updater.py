import logging, requests, sys, traceback, urllib.parse


class BinariesNotFoundError(Exception):
  pass


def update_encoders(target, ssl, encoders):
  ssl = "s" if ssl else ""
  platform = sys.platform

  for encoder in encoders:
    if platform == "win32":
      encoder += ".exe"

    base_url = "http{}://{}/bin/{}/".format(ssl, target, sys.platform)
    url = urllib.parse.urljoin(base_url, encoder)
    logging.info("Downloading", url)

    try:
      with requests.get(url, allow_redirects=True) as r:
        if r.status_code != 200:
          return False

        with open(encoder, "wb+") as f:
          f.write(r.content)
          logging.info("Downloaded", encoder)
    except:
      logging.error(traceback.format_exc())
      return False

  return True
