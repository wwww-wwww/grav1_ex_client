import subprocess

def aom_vpx_encode(encoder, encoder_path, worker, job):
  worker.job_started = time.time()

  encoder_params = job.encoder_params
  ffmpeg_params = job.ffmpeg_params

  if encoder == "aomenc" and "vmaf" in encoder_params and len(worker.client.args.vmaf_path) > 0:
    encoder_params += f" --vmaf-model-path={worker.client.args.vmaf_path}"

  vfs = [f"select=gte(n\\,{job.start})"]

  vf_match = re.search(r"(?:-vf\s\"([^\"]+?)\"|-vf\s([^\s]+?)\s)", ffmpeg_params)

  if vf_match:
    vfs.append(vf_match.group(1) or vf_match.group(2))
    ffmpeg_params = re.sub(r"(?:-vf\s\"([^\"]+?)\"|-vf\s([^\s]+?)\s)", "", ffmpeg_params).strip()

  vf = ",".join(vfs)

  output_filename = f"{job.video}.ivf"

  ffmpeg = [
    worker.client.args.ffmpeg, "-y", "-hide_banner",
    "-loglevel", "error",
    "-i", job.video,
    "-strict", "-1",
    "-pix_fmt", "yuv420p",
    "-vf", vf,
    "-vframes", job.frames
  ]

  if ffmpeg_params:
    ffmpeg.extend(ffmpeg_params.split(" "))

  ffmpeg.extend(["-f", "yuv4mpegpipe", "-"])

  aom = [encoder_path, "-", "--ivf", f"--fpf={job.video}.log", f"--threads={args.threads}", "--passes=2"]

  passes = [
    aom + re.sub(r"--denoise-noise-level=[0-9]+", "", encoder_params).split(" ") + ["--pass=1", "-o", os.devnull],
    aom + encoder_params.split(" ") + ["--pass=2", "-o", output_filename]
  ]

  if job.grain:
    if not job.has_grain:
      return False, None
    else:
      passes[1].append(f"--film-grain-table={job.grain}")

  total_frames = int(job.frames)

  success = True
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
    worker.update_status(f"{encoder:.3s}", "pass:", pass_n, print_progress(0, total_frames), progress=True)

    while True:
      line = worker.pipe.stdout.readline().strip()

      if len(line) == 0 and worker.pipe.poll() is not None:
        break

      match = re.search(r"frame.*?\/([^ ]+?) ", line)
      if match:
        frames = int(match.group(1))
        worker.progress = (pass_n, frames)
        if pass_n == 2:
          worker.update_fps(frames)
        worker.update_status(f"{encoder:.3s}", "pass:", pass_n, print_progress(frames, total_frames), progress=True)

    if ffmpeg_pipe.poll() is None:
      ffmpeg_pipe.kill()

    if worker.pipe.returncode != 0:
      success = False

  if os.path.isfile(f"{job.video}.log"):
    os.remove(f"{job.video}.log")

  return success, output_filename
