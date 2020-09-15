import subprocess, re, os, logging
from util import print_progress

class EncodingException(Exception):
  pass

def aom_vpx_encode(encoder, ffmpeg_path, encoder_path, worker, job):
  encoder_params = job.encoder_params
  ffmpeg_params = job.ffmpeg_params

  #if encoder == "aomenc" and "vmaf" in encoder_params and len(worker.client.args.vmaf_path) > 0:
  #  encoder_params += f" --vmaf-model-path={worker.client.args.vmaf_path}"

  vfs = [f"select=gte(n\\,{job.start})"]

  if "-vf" in ffmpeg_params:
    idx = ffmpeg_params.index("-vf")
    del ffmpeg_params[idx]
    if idx + 1 < len(ffmpeg_params):
      vfs.append(ffmpeg_params[idx + 1])
      del ffmpeg_params[idx + 1]

  vf = ",".join(vfs)

  output_filename = "tmp{}.ivf".format(job.segment)
  log_path = "tmp{}.log".format(job.segment)

  ffmpeg = [
    ffmpeg_path, "-y", "-hide_banner",
    "-loglevel", "error",
    "-i", job.filename,
    "-strict", "-1",
    "-pix_fmt", "yuv420p",
    "-vf", vf,
    "-vframes", job.frames
  ]

  ffmpeg.extend(ffmpeg_params)
  ffmpeg.extend(["-f", "yuv4mpegpipe", "-"])

  ffmpeg = [str(s) for s in ffmpeg]

  aom = [
    encoder_path,
    "-",
    "--ivf",
    f"--threads=8",
    f"--passes={job.passes}"
    "--fpf={}".format(log_path),
  ] + encoder_params

  aom = [str(s) for s in aom]

  if job.passes == 2:
    passes = [
      aom + ["--pass=1", "-o", os.devnull],
      aom + ["--pass=2", "-o", output_filename]
    ]
  else:
    passes = aom + ["-o", output_filename]

  #if job.grain_table:
  #  if not job.has_grain:
  #    return False, None
  #  else:
  #    passes[-1].append(f"--film-grain-table={job.grain}")

  total_frames = job.frames

  for pass_n, cmd in enumerate(passes, start=1):
    ffmpeg_pipe = subprocess.Popen(ffmpeg,
      stdout=subprocess.PIPE,
      stderr=subprocess.STDOUT)

    worker.pipe = subprocess.Popen(cmd,
      stdin=ffmpeg_pipe.stdout,
      stdout=subprocess.PIPE,
      stderr=subprocess.STDOUT,
      universal_newlines=True)

    worker.progress = (pass_n, 0)
    worker.update_status(f"{encoder:.3s}", "pass:", pass_n, print_progress(0, total_frames))

    output = []
    while True:
      line = worker.pipe.stdout.readline().strip()

      if len(line) == 0 and worker.pipe.poll() is not None or worker.cancel_job or worker.stopped:
        break
      
      output.append(line)

      match = re.search(r"frame.*?\/([^ ]+?) ", line)
      if match:
        frames = int(match.group(1))
        worker.progress = (pass_n, frames)
        worker.update_progress()
        worker.update_status(f"{encoder:.3s}", "pass:", pass_n, print_progress(frames, total_frames))

    if ffmpeg_pipe.poll() is None:
      ffmpeg_pipe.kill()

    if worker.pipe.poll() is None:
      worker.pipe.kill()

    if worker.pipe.returncode != 0:
      logging.error("\n".join(output))

      if os.path.exists(output_filename):
        try:
          os.remove(output_filename)
        except: pass

      raise EncodingException()

  if os.path.isfile(log_path):
    try:
      os.remove(log_path)
    except: pass

  return output_filename
