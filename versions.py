import subprocess, shutil, re


def get_version_aomenc(path):
  if not shutil.which(path) and os.path.isfile(path):
    return None
  p = subprocess.run([path, "--help"], stdout=subprocess.PIPE)
  r = re.search(r"av1.+?\s+([0-9][^\s]+)", p.stdout.decode("utf-8"))
  return r.group(1).strip()


def get_version_vpxenc(path):
  if not shutil.which(path) and os.path.isfile(path):
    return None
  p = subprocess.run([path, "--help"], stdout=subprocess.PIPE)
  r = re.search(r"vp9.+?\s+(v[^\s]+)", p.stdout.decode("utf-8"))
  return r.group(1).strip()


get_versions = {
  "aomenc": get_version_aomenc,
  "vpxenc": get_version_vpxenc,
}


def get_version(encoder, path):
  try:
    return get_versions[encoder](path)
  except:
    return None
