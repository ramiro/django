import functools
import itertools
import os
import pathlib
import signal
import subprocess
import sys
import threading
import time
import traceback
from collections import defaultdict
from pathlib import Path
from types import ModuleType
from zipimport import zipimporter

from django.apps import apps
from django.core.signals import request_finished
from django.dispatch import Signal

autoreload_started = Signal()
file_changed = Signal(providing_args=['path', 'kind'])

DJANGO_AUTORELOAD_ENV = 'RUN_MAIN'

# If an error is raised while importing a file, it is not placed
# in sys.modules. This means any future modifications are not
# caught. We keep a list of these file paths to continue to
# watch them in the future.
_error_files = []
_exception = None

try:
    import termios
except ImportError:
    termios = None

USE_INOTIFY = False
try:
    # Test whether inotify is enabled and likely to work
    import pyinotify

    fd = pyinotify.INotifyWrapper.create().inotify_init()
    if fd >= 0:
        USE_INOTIFY = True
        os.close(fd)
except ImportError:
    pass


def ensure_echo_on():
    if termios:
        fd = sys.stdin
        if fd.isatty():
            attr_list = termios.tcgetattr(fd)
            if not attr_list[3] & termios.ECHO:
                attr_list[3] |= termios.ECHO
                if hasattr(signal, 'SIGTTOU'):
                    old_handler = signal.signal(signal.SIGTTOU, signal.SIG_IGN)
                else:
                    old_handler = None
                termios.tcsetattr(fd, termios.TCSANOW, attr_list)
                if old_handler is not None:
                    signal.signal(signal.SIGTTOU, old_handler)


def iter_all_python_module_files():
    # Explicitly pass in modules and extra_files here to allow lru_cache
    # to return the same result if no files have been changed, preventing
    # needlessly creating many Pathlib instances every time BaseReloader.watched_files
    # is called.
    return set(iter_modules_and_files(sys.modules.values(), frozenset(_error_files)))


@functools.lru_cache(maxsize=1)
def iter_modules_and_files(modules, extra_files):
    sys_file_paths = []
    for module in modules:
        # During debugging (with PyDev) the objects 'typing.io' and 'typing.re' are added to sys.modules,
        # however they are types not modules and so cause issues here.
        if not isinstance(module, ModuleType) or module.__spec__ is None:
            continue
        spec = module.__spec__
        # Modules could be loaded from zip files or other locations
        # that we cannot yet reload on
        if spec.has_location:
            if isinstance(spec.loader, zipimporter):
                origin = spec.loader.archive
            else:
                origin = spec.origin
            sys_file_paths.append(origin)

    for filename in itertools.chain(sys_file_paths, extra_files):
        if not filename:
            continue

        path = pathlib.Path(filename)
        if not path.exists():
            # The module could have been removed, do not fail loudly if this is the case.
            continue
        yield path.resolve().absolute()


def raise_last_exception():
    global _exception
    if _exception is not None:
        raise _exception[0](_exception[1]).with_traceback(_exception[2])


def check_errors(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        global _exception
        try:
            fn(*args, **kwargs)
        except Exception:
            _exception = sys.exc_info()

            et, ev, tb = _exception

            if getattr(ev, 'filename', None) is None:
                # get the filename from the last item in the stack
                filename = traceback.extract_tb(tb)[-1][0]
            else:
                filename = ev.filename

            if filename not in _error_files:
                _error_files.append(filename)

            raise

    return wrapper


def get_child_arguments():
    """
    Returns the executable. This contains a workaround for windows
    if the executable is incorrectly reported to not have the .exe
    extension which can cause bugs on reloading.
    """
    import django.__main__

    args = [sys.executable] + ['-W%s' % o for o in sys.warnoptions]
    if sys.argv[0] == django.__main__.__file__:
        # The server was started with `python -m django runserver`.
        args += ['-m', 'django']
        args += sys.argv[1:]
    else:
        args += sys.argv

    return args


def trigger_reload(filename, kind='changed'):
    print('{0} {1}, reloading'.format(filename, kind))
    sys.exit(3)


def restart_with_reloader():
    new_environ = {**os.environ, DJANGO_AUTORELOAD_ENV: 'true'}
    args = get_child_arguments()

    while True:
        exit_code = subprocess.call(args, env=new_environ, close_fds=False)

        if exit_code != 3:
            return exit_code


class BaseReloader:
    def __init__(self):
        self.extra_files = set()
        self.directory_globs = defaultdict(set)
        self._stop_condition = threading.Event()

    def watch_dir(self, path, glob):
        path = Path(path)
        if not path.is_absolute():
            raise ValueError('{0} must be absolute.'.format(path))

        self.directory_globs[path].add(glob)

    def watch_file(self, path):
        path = Path(path)
        if not path.is_absolute():
            raise ValueError('{0} must be absolute.'.format(path))

        self.extra_files.add(path)

    def watched_files(self, include_globs=True):
        yield from iter_all_python_module_files()
        yield from self.extra_files

        if include_globs:
            for directory, patterns in self.directory_globs.items():
                for pattern in patterns:
                    yield from directory.glob(pattern)

    def run(self):
        while not apps.ready:
            time.sleep(0.1)

        autoreload_started.send(sender=self)
        self.run_loop()

    def run_loop(self):
        raise NotImplementedError('BaseReloader subclasses must implement run_loop.')

    @classmethod
    def is_available(cls):
        raise NotImplementedError()

    def notify_file_changed(self, path):
        results = file_changed.send(sender=self, file_path=path)
        if not any(res[1] for res in results):
            trigger_reload(path)

    # These are primarily used for testing
    @property
    def should_stop(self):
        return self._stop_condition.is_set()

    def stop(self):
        self._stop_condition.set()


class StatReloader(BaseReloader):
    def run_loop(self):
        file_times = {}

        while True:
            file_times.update(self.loop_files(file_times))
            if self.should_stop:
                return

            time.sleep(1)

    def loop_files(self, previous_times):
        updated_times = {}
        for path, mtime in self.snapshot_files():
            previous_time = previous_times.get(path)

            # If there are overlapping globs then a file
            # may be iterated twice.
            if path in updated_times:
                continue

            if previous_time is None:
                updated_times[path] = mtime

            elif previous_time != mtime:
                self.notify_file_changed(path)
                updated_times[path] = mtime
        return updated_times

    def snapshot_files(self):
        for file in self.watched_files():
            try:
                mtime = file.stat().st_mtime
            except OSError:
                continue

            yield file, mtime

    @classmethod
    def is_available(cls):
        return True


class InotifyReloader(BaseReloader):
    def update_watcher(self, wm, sender, **kwargs):
        if sender and getattr(sender, 'handles_files', False):
            # No need to update watches when request serves files.
            # (sender is supposed to be a django.core.handlers.BaseHandler subclass)
            return
        mask = (
            pyinotify.IN_MODIFY |
            pyinotify.IN_DELETE |
            pyinotify.IN_ATTRIB |
            pyinotify.IN_MOVED_FROM |
            pyinotify.IN_MOVED_TO |
            pyinotify.IN_CREATE |
            pyinotify.IN_DELETE_SELF |
            pyinotify.IN_MOVE_SELF
        )
        for path in self.watched_files():
            wm.add_watch(str(path), mask, rec=path.is_dir())

    def run_loop(self):
        wm = pyinotify.WatchManager()

        class EventHandler(pyinotify.ProcessEvent):
            def __init__(self, reloader):
                self.reloader = reloader

            def match_file(self, parent, file):
                if parent in self.reloader.recursive_globs:
                    for glob in self.reloader.directory_globs[parent]:
                        if file.match(glob):
                            return True

            def process_default(self, event):
                file_path = Path(event.path)
                if file_path in self.reloader.extra_files:
                    return self.reloader.notify_file_changed(file_path)
                # It must be a glob. Find if it matches any of our globs
                for parent in file_path.parents:
                    if self.match_file(parent, file_path):
                        self.reloader.notify_file_changed(file_path)

        notifier = pyinotify.Notifier(wm, EventHandler(self))

        request_finished.connect(functools.partial(self.update_watcher, wm))
        self.update_watcher(wm, None)

        while True:
            notifier.check_events(timeout=1)
            notifier.read_events()
            notifier.process_events()
            if self.should_stop:
                notifier.stop()
                return

    @classmethod
    def is_available(cls):
        return USE_INOTIFY


def get_reloader():
    if InotifyReloader.is_available():
        return InotifyReloader()

    return StatReloader()


def start_django(reloader, main_func, *args, **kwargs):
    ensure_echo_on()

    main_func = check_errors(main_func)
    thread = threading.Thread(target=main_func, args=args, kwargs=kwargs)
    thread.setDaemon(True)
    thread.start()

    reloader.run()


def run_with_reloader(main_func, *args, **kwargs):
    signal.signal(signal.SIGTERM, lambda *args: sys.exit(0))
    reloader = get_reloader()
    try:
        if os.environ.get(DJANGO_AUTORELOAD_ENV) == 'true':
            start_django(reloader, main_func, *args, **kwargs)
        else:
            print('Watching for file changes with {0}'.format(reloader.__class__.__name__))
            exit_code = restart_with_reloader()
            sys.exit(exit_code)
    except KeyboardInterrupt:
        pass
