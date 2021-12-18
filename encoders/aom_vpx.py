import re, os, logging, traceback, subprocess, shutil
from util import print_progress, JobStopped, EncodingError
from .encoder import Encoder


def encode(encoder, threads, ffmpeg_path, encoder_path, job):
  encoder_params = job.encoder_params
  ffmpeg_params = job.ffmpeg_params

  vfs = [f"select=gte(n\\,{job.start})"]

  if "-vf" in ffmpeg_params:
    idx = ffmpeg_params.index("-vf")
    del ffmpeg_params[idx]
    if idx + 1 < len(ffmpeg_params):
      vfs.append(ffmpeg_params[idx + 1])
      del ffmpeg_params[idx + 1]

  vf = ",".join(vfs)

  output_filename = f"tmp{job.segment}.ivf"
  log_path = f"tmp{job.segment}.log"

  ffmpeg = [
    ffmpeg_path,
    "-y",
    "-loglevel",
    "error",
    "-i",
    job.filename,
    "-strict",
    "-1",
    "-pix_fmt",
    "yuv420p10le",
    "-vsync",
    "0",
    "-vf",
    vf,
    "-vframes",
    job.frames,
  ]

  ffmpeg.extend(ffmpeg_params)
  ffmpeg.extend(["-f", "yuv4mpegpipe", "-"])

  ffmpeg = [str(s) for s in ffmpeg]

  aom = [
    encoder_path,
    "-",
    "--ivf",
    f"--fpf={log_path}",
    f"--threads={threads}",
    f"--passes={job.passes}",
    "-o",
    output_filename,
  ] + encoder_params

  aom = [str(s) for s in aom]

  if job.passes == 2:
    pass1 = [a for a in aom if not a.startswith("--denoise-noise-level")]
    passes = [
      pass1 + ["--pass=1"],
      aom + ["--pass=2"],
    ]

  # if job.grain_table:
  #  if not job.has_grain:
  #    return False, None
  #  else:
  #    passes[-1].append(f"--film-grain-table={job.grain}")

  total_frames = job.frames

  ffmpeg_pipe = None

  try:
    for pass_n, cmd in enumerate(passes, start=1):
      ffmpeg_pipe = subprocess.Popen(
        ffmpeg,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
      )

      job.pipe = subprocess.Popen(
        cmd,
        stdin=ffmpeg_pipe.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
      )

      job.progress = (pass_n, 0)
      job.update_status(
        "{:.3s}".format(encoder),
        "pass:",
        pass_n,
        print_progress(0, total_frames),
      )

      output = []
      while True:
        line = job.pipe.stdout.readline().strip()

        if len(line) == 0 and job.pipe.poll() is not None:
          break

        if job.stopped:
          raise JobStopped

        if len(line) > 0:
          output.append(line)

          match = re.search(r"frame.*?\/([^ ]+?) ", line)
          if match:
            frames = int(match.group(1))
            job.progress = (pass_n, frames)
            job.update_progress()
            job.update_status(
              "{:.3s}".format(encoder),
              "pass:",
              pass_n,
              print_progress(frames, total_frames),
            )

      if job.pipe.returncode != 0:
        if os.path.exists(output_filename):
          try:
            os.remove(output_filename)
          except:
            pass

        raise EncodingError("\n".join(output))

  except JobStopped:
    logging.info("Job stopped")
    return None
  except:
    logging.error(traceback.format_exc())
    return None
  finally:
    if ffmpeg_pipe:
      ffmpeg_pipe.kill()
    job.pipe.kill()

    if os.path.isfile(log_path):
      try:
        os.remove(log_path)
      except:
        pass

  return output_filename


class Aomenc(Encoder):
  def get_version(self, paths):
    path = paths["aomenc"]
    if not shutil.which(path):
      return None
    p = subprocess.run([path, "--help"], stdout=subprocess.PIPE)
    r = re.search(r"av1.+?\s+([0-9][^\s]+)", p.stdout.decode("utf-8"))
    self.version = r.group(1).strip() if r else None
    return self.version

  def encode(self, job, paths, threads):
    return encode("aom", threads, paths["ffmpeg"], paths["aomenc"], job)


class Vpxenc(Encoder):
  def get_version(self, paths):
    path = paths["vpxenc"]
    if not shutil.which(path):
      return None
    p = subprocess.run([path, "--help"], stdout=subprocess.PIPE)
    r = re.search(r"vp9.+?\s+(v[^\s]+)", p.stdout.decode("utf-8"))
    self.version = r.group(1).strip() if r else None
    return self.version

  def encode(self, job, paths, threads):
    return encode("vpx", threads, paths["ffmpeg"], paths["vpxenc"], job)
