import curses, textwrap, logging, traceback
from threading import Event, Lock, Thread

KEY_TAB = ord("\t")
KEY_R = ord("R")


class Tab:
  def __init__(self, screen, name):
    self.screen = screen
    self.name = name

  def header(self, cols):
    return ""

  def footer(self, cols):
    return ""

  def render(self, cols, rows):
    return []

  def on_key(self, key):
    pass

  def refresh(self):
    self.screen.refresh_tab(self.name)


class WorkerTab(Tab):
  def __init__(self, screen, client):
    super().__init__(screen, "Workers")
    self.client = client
    self.scroll = 0

  def on_key(self, key):
    if key == curses.KEY_UP:
      self.scroll -= 1
      self.refresh()
    elif key == curses.KEY_DOWN:
      self.scroll += 1
      self.refresh()

  def header(self, cols):
    active_workers = len(self.client.workers.working)

    if self.client.segment_store.downloading:
      downloading = "Downloading: {:.2f}%".format(
        self.client.segment_store.download_progress * 100)
    else:
      downloading = ""

    return "Workers: {} Active: {} Queue: {} Uploading: {} Hit: {} Miss: {} {}".format(
      self.client.workers.max_workers, active_workers,
      len(self.client.workers.work_queue.queue),
      len(self.client.upload_queue.work_queue.queue) +
      len(self.client.upload_queue.working), self.client.hit, self.client.miss,
      downloading)

  def render(self, cols, rows):
    body = []

    self.scroll = max(
      min(self.scroll,
          len(self.client.workers.working) - rows), 0)

    for k, worker in enumerate(
        self.client.workers.working[self.scroll:rows + self.scroll],
        1 + self.scroll):
      job = worker.args[0]
      body.append("{:2d} {}".format(k, job.status))

    return body


class LogTab(Tab):
  def __init__(self, screen, logger):
    super().__init__(screen, "Log")
    self.logger = logger
    self.scroll = 0

    logger.on_message = self.on_message

  def on_message(self, message):
    self.refresh()

  def header(self, cols):
    left = "Log"
    right = "{}/{}".format(self.scroll, len(self.logger.messages))
    return "{}{}{}".format(left, ' ' * max(cols - len(left + right), 1), right)

  def render(self, cols, rows):
    text = [message.msg for message in self.logger.messages[-rows:]]
    text = [line for lines in [t.split("\n") for t in text] for line in lines]
    return [
      line for lines in [textwrap.wrap(line, width=cols) for line in text]
      for line in lines
    ][-rows:]


class Screen:
  def __init__(self, client):
    self.tab = 0
    self.tabs = []
    self.scr = None

    self.render_lock = Lock()
    self.refresh = Event()

    self.client = client

    Thread(target=self.screen, daemon=True).start()

  def add_tab(self, tab):
    self.tabs.append(tab)

  def refresh_tab(self, tab):
    if self.tabs[self.tab].name == tab:
      self.refresh_screen()

  def screen(self):
    while self.refresh.wait():
      if not self.scr: continue
      if self.tab >= len(self.tabs): continue
      with self.render_lock:
        try:
          tab = self.tabs[self.tab]

          (mlines, mcols) = self.scr.getmaxyx()

          header_text = tab.header(mcols)
          footer_text = tab.footer(mcols)

          header = [line for line in textwrap.wrap(header_text, width=mcols)]
          footer = [line for line in textwrap.wrap(footer_text, width=mcols)]

          footer2 = " ".join(
            ["F{} {} ".format(i, t.name) for i, t in enumerate(self.tabs, 1)])
          footer2_right = "F12 Quit"
          footer2 = "{}{}{}".format(
            footer2, ' ' * max(mcols - len(footer2 + footer2_right), 1),
            footer2_right)
          footer2 = [line for line in textwrap.wrap(footer2, width=mcols)]

          body = tab.render(mcols,
                            mlines - len(header) - len(footer) - len(footer2))

          self.scr.erase()

          for i, line in enumerate(header):
            self.scr.insstr(i, 0, line.ljust(mcols), curses.color_pair(1))

          for i, line in enumerate(body, len(header)):
            self.scr.insstr(i, 0, line)

          for i, line in enumerate(footer,
                                   mlines - len(footer) - len(footer2)):
            self.scr.insstr(i, 0, line.ljust(mcols), curses.color_pair(1))

          for i, line in enumerate(footer2, mlines - len(footer2)):
            self.scr.insstr(i, 0, line, curses.color_pair(1))

          self.scr.refresh()
        except:
          logging.error(traceback.format_exc())

      self.refresh.clear()

  def refresh_screen(self):
    self.refresh.set()

  def key_loop(self, scr):
    while True:
      c = scr.getch()

      if self.tab >= len(self.tabs) or len(self.tabs) == 0: continue

      if c == curses.KEY_F12 or c == ord("q"):
        self.client.stop()
        return

      if c == curses.KEY_F1:
        self.tab = 0
        with self.render_lock:
          self.scr.clear()
          self.refresh_screen()
        continue

      if c == curses.KEY_F2:
        self.tab = 1
        with self.render_lock:
          self.scr.clear()
          self.refresh_screen()
        continue

      if c == KEY_R:
        with self.render_lock:
          self.scr.clear()
          self.scr.refresh()
          self.refresh.set()
        continue

      self.tabs[self.tab].on_key(c)
      self.refresh_screen()

  def window(self, scr):
    self.scr = scr

    curses.curs_set(0)
    scr.nodelay(0)

    curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_WHITE)

    self.refresh_screen()

    Thread(target=self.key_loop, args=(scr, ), daemon=True).start()

    self.client.exit_event.wait()

    curses.curs_set(1)

  def attach(self):
    curses.wrapper(self.window)
