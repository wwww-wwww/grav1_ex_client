try:
  from .encoders.aom_vpx import Aomenc, Vpxenc
except:
  from encoders.aom_vpx import Aomenc, Vpxenc


def get_encoders(paths):
  return {
    "aomenc": Aomenc(paths),
    "vpxenc": Vpxenc(paths),
  }


def get_versions(encoders):
  return {k: v.version for k, v in encoders.items()}
