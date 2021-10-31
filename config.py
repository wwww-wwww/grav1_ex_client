import os
from argparse import ArgumentParser
from yaml import load

try:
  from yaml import CLoader as Loader
except ImportError:
  from yaml import Loader


class Args(dict):
  def __init__(self):
    self._parser = ArgumentParser()
    self._default_args = {}

  def add_argument(self, *args, default=None, default2=None, **kwargs):
    if default is not None:
      self._default_args[args[0]] = default
    self._parser.add_argument(*args, default=default2, **kwargs)

  def parse(self):
    args = self._parser.parse_args().__dict__
    for key in args:
      self[key] = args[key]

    return self

  def update(self, args):
    for key in args:
      if key not in self or self[key] == None:
        self[key] = args[key]

    for key in self._default_args:
      vkey = key
      while vkey[0] == "-":
        vkey = vkey[1:]

      if self[vkey] == None:
        self[vkey] = self._default_args[key]

  def __str__(self):
    keys = self.keys()
    return ", ".join(["{}: {}".format(key, self[key]) for key in keys])

  __getattr__ = dict.get
  __setattr__ = dict.__setitem__
  __delattr__ = dict.__delitem__


def load_config():
  args = Args()

  args.add_argument("-c",
                    "--config",
                    default2="config.yaml",
                    help="Path to config (default: config.yaml)")
  args.add_argument("target", type=str, nargs="?", default="localhost:4000")
  args.add_argument("--key", type=str, help="API key")
  args.add_argument("--workers", type=int, default=1)
  args.add_argument("--threads", type=int, default=8)
  args.add_argument("--queue", type=int, default=3)
  args.add_argument("--name", type=str, help="Name of the client")
  args.add_argument("--hostname",
                    help="Use hostname as name",
                    action="store_true")
  args.add_argument("--aomenc",
                    default="aomenc",
                    help="Path to aomenc (default: aomenc)")
  args.add_argument("--vpxenc",
                    default="vpxenc",
                    help="Path to vpxenc (default: vpxenc)")
  args.add_argument("--ffmpeg",
                    default="ffmpeg",
                    help="Path to ffmpeg (default: ffmpeg)")
  args.add_argument("--alt-dl-server",
                    help="Location to external download server")
  args.add_argument("--headless",
                    help="Launch without UI",
                    action="store_true")

  args.parse()

  if os.path.isfile(args.config):
    args.update(load(open(args.config), Loader=Loader))
  else:
    args.update({})

  if not args.key:
    print("target: ", args.target)
    args.key = input("key: ")

  if not args.key:
    print("key must be provided")
    exit(1)

  return args
