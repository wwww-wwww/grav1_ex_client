import curses, textwrap
from threading import Event, Lock, Thread

KEY_TAB = ord("\t")
KEY_R = ord("R")

class Tab:
  def __init__(self, screen): pass
  def header(self, cols): return ""
  def footer(self, cols): return ""
  def render(self, cols, rows): return []
  def on_key(self, key): pass

class WorkerTab(Tab):
  def __init__(self, screen, workers):
    self.name = "Workers"
    self.screen = screen
    self.workers = workers
  
  def header(self, cols):
    return f"Workers: {len(self.workers)}"

class LogTab(Tab):
  def __init__(self, screen, logger):
    self.name = "Log"
    self.screen = screen
    self.logger = logger
    self.scroll = 0

    logger.on_message = self.on_message

  def on_message(self, message):
    self.screen.refresh_screen()

  def header(self, cols):
    left = "Log"
    right = f"{self.scroll}/{len(self.logger.messages)}"
    return f"{left}{' ' * max(cols - len(left + right), 1)}{right}"

  def render(self, cols, rows):
    text = [message.msg for message in self.logger.messages[-rows:]]
    return [line for lines in [textwrap.wrap(line, width=cols) for line in text] for line in lines][-rows:]

class Screen:
  def __init__(self, exit_event):
    self.tab = 0
    self.tabs = []
    self.scr = None

    self.render_lock = Lock()
    self.refresh = Event()

    self.exit_event = exit_event

    Thread(target=self.screen, daemon=True).start()

  def add_tab(self, tab):
    self.tabs.append(tab)

  def screen(self):
    while self.refresh.wait():
      if not self.scr: continue
      if self.tab >= len(self.tabs): continue
      with self.render_lock:
        tab = self.tabs[self.tab]

        (mlines, mcols) = self.scr.getmaxyx()

        header_text = tab.header(mcols)
        footer_text = tab.footer(mcols)

        header = [line for line in textwrap.wrap(header_text, width=mcols)]
        footer = [line for line in textwrap.wrap(footer_text, width=mcols)]
        
        footer2 = " ".join([f"F{i} {t.name} " for i, t in enumerate(self.tabs, 1)])
        footer2_right = "F12 Quit"
        footer2 = f"{footer2}{' ' * max(mcols - len(footer2 + footer2_right), 1)}{footer2_right}"
        footer2 = [line for line in textwrap.wrap(footer2, width=mcols)]

        body = tab.render(mcols, mlines - len(header) - len(footer) - len(footer2))

        self.scr.erase()

        for i, line in enumerate(header):
          self.scr.insstr(i, 0, line.ljust(mcols), curses.color_pair(1))

        for i, line in enumerate(body, len(header)):
          self.scr.insstr(i, 0, line)

        for i, line in enumerate(footer, mlines - len(footer) - len(footer2)):
          self.scr.insstr(i, 0, line, curses.color_pair(1))

        for i, line in enumerate(footer2, mlines - len(footer2)):
          self.scr.insstr(i, 0, line, curses.color_pair(1))

        self.scr.refresh()
      self.refresh.clear()

  def refresh_screen(self):
    self.refresh.set()

  def key_loop(self, scr):
    while True:
      c = scr.getch()

      if self.tab >= len(self.tabs) or len(self.tabs) == 0: continue

      if c == curses.KEY_F12:
        self.exit_event.set()
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
        continue
      
      self.tabs[self.tab].on_key(c)
      self.refresh_screen()

  def window(self, scr):
    self.scr = scr

    curses.curs_set(0)
    scr.nodelay(0)

    curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_WHITE)

    self.refresh_screen()
    
    Thread(target=self.key_loop, args=(scr,), daemon=True).start()

    self.exit_event.wait()

    curses.curs_set(1)

  def attach(self):
    curses.wrapper(self.window)
