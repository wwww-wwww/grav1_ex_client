import logging, os, requests, sys, traceback
from urllib.parse import urljoin


class BinariesNotFoundError(Exception):
  pass


def update_encoders(target, encoders):
  platform = sys.platform
  base_url = urljoin(target, "bin/{}/".format(sys.platform))

  if platform == "linux":
    import stat

  for encoder in encoders:
    if platform == "win32":
      encoder += ".exe"

    url = urljoin(base_url, encoder)
    logging.info("Downloading", url)

    try:
      with requests.get(url, allow_redirects=True) as r:
        if r.status_code != 200:
          return False

        with open(encoder, "wb+") as f:
          f.write(r.content)
          logging.info("Downloaded", encoder)

        if platform == "linux":
          os.chmod(encoder, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

    except:
      logging.error(traceback.format_exc())
      return False

  return True
