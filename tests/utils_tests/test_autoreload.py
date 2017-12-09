import contextlib
import py_compile
import shutil
import tempfile
import time
import zipfile
from importlib import import_module
from pathlib import Path
from threading import Thread
from unittest import mock, skipUnless

from django.dispatch import receiver
from django.test import SimpleTestCase
from django.test.utils import extend_sys_path
from django.utils import autoreload


class TestFilenameGenerator(SimpleTestCase):

    def clear_autoreload_caches(self):
        autoreload.iter_modules_and_files.cache_clear()

    def assertFileFound(self, filename):
        # Some temp directories are symlinks. Python resolves these fully while importing.
        resolved_filename = filename.resolve()
        self.clear_autoreload_caches()
        # Test uncached access
        self.assertIn(resolved_filename, list(autoreload.iter_all_python_module_files()))
        # Test cached access
        self.assertIn(resolved_filename, list(autoreload.iter_all_python_module_files()))

    def assertFileNotFound(self, filename):
        resolved_filename = filename.resolve()
        self.clear_autoreload_caches()
        # Test uncached access
        self.assertNotIn(resolved_filename, list(autoreload.iter_all_python_module_files()))
        # Test cached access
        self.assertNotIn(resolved_filename, list(autoreload.iter_all_python_module_files()))

    def temporary_file(self, filename):
        dirname = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, dirname)
        return Path(dirname) / filename

    def test_paths_are_pathlib_instances(self):
        for filename in autoreload.iter_all_python_module_files():
            self.assertIsInstance(filename, Path)

    def test_file_added(self):
        """
        When a file is added it is returned by iter_all_python_module_files()
        """
        filename = self.temporary_file('test_deleted_removed_module.py')
        filename.touch()

        with extend_sys_path(str(filename.parent)):
            import_module('test_deleted_removed_module')

        self.assertFileFound(filename.absolute())

    def test_check_errors(self):
        """
        When a file containing an error is imported in a function wrapped by
        check_errors(), gen_filenames() returns it.
        """
        filename = self.temporary_file('test_syntax_error.py')
        filename.write_text("Ceci n'est pas du Python.")

        with extend_sys_path(str(filename.parent)):
            with self.assertRaises(SyntaxError):
                autoreload.check_errors(import_module)('test_syntax_error')
        self.assertFileFound(filename)

    def test_check_errors_catches_all_exceptions(self):
        """
        Since Python may raise arbitrary exceptions when importing code,
        check_errors() must catch Exception, not just some subclasses.
        """
        filename = self.temporary_file('test_exception.py')
        filename.write_text('raise Exception')

        with extend_sys_path(str(filename.parent)):
            with self.assertRaises(Exception):
                autoreload.check_errors(import_module)('test_exception')
        self.assertFileFound(filename)

    def test_zip_reload(self):
        """
        Modules imported from zipped files should have their archive location
        included in the result
        """
        zip_file = self.temporary_file('zip_import.zip')
        with zipfile.ZipFile(str(zip_file), 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr('test_zipped_file.py', '')

        with extend_sys_path(str(zip_file)):
            import_module('test_zipped_file')
        self.assertFileFound(zip_file)

    def test_bytecode_conversion_to_source(self):
        """
        .pyc and .pyo files should be included in the files list
        """
        filename = self.temporary_file('test_compiled.py')
        filename.touch()
        compiled_file = Path(py_compile.compile(str(filename), str(filename.with_suffix('.pyc'))))
        filename.unlink()
        with extend_sys_path(str(compiled_file.parent)):
            import_module('test_compiled')
        self.assertFileFound(compiled_file)


class RestartWithReloaderTests(SimpleTestCase):
    executable = '/usr/bin/python'

    def patch_autoreload(self, argv):
        patch_call = mock.patch('django.utils.autoreload.subprocess.call', return_value=0)
        patches = [
            mock.patch('django.utils.autoreload.sys.argv', argv),
            mock.patch('django.utils.autoreload.sys.executable', self.executable),
            mock.patch('django.utils.autoreload.sys.warnoptions', ['all']),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        mock_call = patch_call.start()
        self.addCleanup(patch_call.stop)
        return mock_call

    def test_manage_py(self):
        argv = ['./manage.py', 'runserver']
        mock_call = self.patch_autoreload(argv)
        autoreload.restart_with_reloader()
        self.assertEqual(mock_call.call_count, 1)
        self.assertEqual(mock_call.call_args[0][0], [self.executable, '-Wall'] + argv)

    def test_python_m_django(self):
        main = '/usr/lib/pythonX.Y/site-packages/django/__main__.py'
        argv = [main, 'runserver']
        mock_call = self.patch_autoreload(argv)
        with mock.patch('django.__main__.__file__', main):
            autoreload.restart_with_reloader()
            self.assertEqual(mock_call.call_count, 1)
            self.assertEqual(mock_call.call_args[0][0], [self.executable, '-Wall', '-m', 'django'] + argv[1:])


class ReloaderTests(SimpleTestCase):
    def setUp(self):
        self._tempdir = tempfile.TemporaryDirectory()
        self.tempdir = Path(self._tempdir.name).absolute()
        self.existing_file = (self.tempdir / 'test.py').absolute()
        self.existing_file.touch()
        self.non_existing_file = (self.tempdir / 'does_not_exist.py').absolute()

    def tearDown(self):
        self._tempdir.cleanup()


class BaseReloaderTests(ReloaderTests):
    def setUp(self):
        super().setUp()
        self.reloader = autoreload.BaseReloader()

    def test_watch_without_absolute(self):
        with self.assertRaises(ValueError, msg='test.py must be absolute.'):
            self.reloader.watch_file('test.py')

    def test_watch_without_recursive(self):
        with self.assertRaises(ValueError, msg='Use recursive=True for recursive globs.'):
            self.reloader.watch_dir('/', glob='**/*.py')

    def test_watch_with_single_file(self):
        self.reloader.watch_file(self.existing_file)
        watched_files = list(self.reloader.watched_files())
        self.assertIn(self.existing_file, watched_files)

    def test_watch_with_glob(self):
        self.reloader.watch_dir(self.tempdir, '*.py')
        watched_files = list(self.reloader.watched_files())
        self.assertIn(self.existing_file, watched_files)

    def test_watch_files_with_recursive_glob(self):
        inner_dir = self.tempdir / 'test'
        inner_dir.mkdir()
        inner_file = inner_dir / 'test.py'
        inner_file.touch()
        self.reloader.watch_dir(self.tempdir, '**/*.py', recursive=True)
        watched_files = list(self.reloader.watched_files())
        self.assertIn(self.existing_file, watched_files)
        self.assertIn(inner_file, watched_files)


class StatReloaderTests(ReloaderTests):
    def setUp(self):
        super().setUp()
        self.reloader = autoreload.StatReloader()

    def test_snapshot_files_ignores_missing_files(self):
        with mock.patch.object(self.reloader, 'watched_files', return_value=[self.non_existing_file]):
            self.assertDictEqual(dict(self.reloader.snapshot_files()), {})

    def test_snapshot_files_updates(self):
        with mock.patch.object(self.reloader, 'watched_files', return_value=[self.existing_file]):
            snapshot1 = dict(self.reloader.snapshot_files())
            self.assertIn(self.existing_file, snapshot1)
            # Sleep long enough for the mtime to have a noticeable change
            time.sleep(0.1)
            self.existing_file.touch()
            snapshot2 = dict(self.reloader.snapshot_files())
            self.assertNotEqual(snapshot1[self.existing_file], snapshot2[self.existing_file])

    def test_does_not_fire_without_changes(self):
        with mock.patch.object(self.reloader, 'watched_files', return_value=[self.existing_file]), \
                mock.patch.object(self.reloader, 'notify_file_changed') as notifier:
            initial_snapshot = self.reloader.loop_files({})
            self.assertCountEqual(initial_snapshot.keys(), [self.existing_file])
            second_snapshot = self.reloader.loop_files(initial_snapshot)
            self.assertDictEqual(second_snapshot, {})
            notifier.assert_not_called()

    def test_does_not_fire_when_created(self):
        with mock.patch.object(self.reloader, 'watched_files', return_value=[self.non_existing_file]), \
                mock.patch.object(self.reloader, 'notify_file_changed') as notifier:
            initial_snapshot = self.reloader.loop_files({})
            self.assertDictEqual(initial_snapshot, {})
            self.non_existing_file.touch()
            second_snapshot = self.reloader.loop_files({})
            self.assertCountEqual(second_snapshot.keys(), [self.non_existing_file])
            notifier.assert_not_called()

    def test_fires_with_changes(self):
        with mock.patch.object(self.reloader, 'watched_files', return_value=[self.existing_file]), \
                mock.patch.object(self.reloader, 'notify_file_changed') as notifier:
            initial_snapshot = {self.existing_file: 1}
            second_snapshot = self.reloader.loop_files(initial_snapshot)
            notifier.assert_called_once_with(self.existing_file)
            self.assertCountEqual(second_snapshot.keys(), [self.existing_file])


@skipUnless(autoreload.InotifyReloader.is_available(), 'pyinotify is not installed')
class PyInotifyReloaderTests(ReloaderTests):
    def setUp(self):
        super().setUp()
        self.reloader = autoreload.InotifyReloader()

    def test_update_watcher_calls_add_watch(self):
        with mock.patch.object(self.reloader, 'watched_files', return_value=[self.existing_file]):
            wm = mock.MagicMock()
            self.reloader.update_watcher(wm, None)
            wm.add_watch.assert_called_once_with(self.existing_file, mock.ANY, rec=False)

    def test_update_watcher_calls_add_watch_recursively(self):
        with mock.patch.object(self.reloader, 'watched_files', return_value=[self.tempdir]):
            wm = mock.MagicMock()
            self.reloader.update_watcher(wm, None)
            wm.add_watch.assert_called_once_with(self.existing_file, mock.ANY, rec=True)

    def test_update_watcher_ignores_staticfiles(self):
        wm = mock.MagicMock()
        sender = mock.MagicMock()
        sender.handles_files = True
        self.reloader.update_watcher(None, sender)
        wm.add_watch.assert_not_called()


class ReloaderIntegrationTests(ReloaderTests):
    @contextlib.contextmanager
    def start_reloader(self, reloader):
        files_changed_calls = []

        @receiver(autoreload.file_changed)
        def files_changed_handler(sender, file_path, **kwargs):
            files_changed_calls.append(file_path)
            return True

        self.addCleanup(lambda: autoreload.file_changed.disconnect(files_changed_handler))
        # In case the thread never terminates.
        self.addCleanup(reloader.stop)
        watch_thread = Thread(target=reloader.run)
        watch_thread.daemon = True
        watch_thread.start()
        time.sleep(1)
        yield files_changed_calls
        time.sleep(1)
        reloader.stop()
        watch_thread.join(2)
        # The thread should now be terminated (sys.exit kills the thread)
        self.assertFalse(watch_thread.is_alive())

    @property
    def reloaders(self):
        res = [autoreload.StatReloader()]
        if autoreload.InotifyReloader.is_available():
            res.append(autoreload.InotifyReloader())
        return res

    def test_file(self):
        for reloader in self.reloaders:
            with self.subTest(reloader=reloader.__class__):
                reloader.watch_file(self.existing_file)
                with self.start_reloader(reloader) as results:
                    self.existing_file.touch()
                self.assertCountEqual(results, [self.existing_file])

    def test_glob(self):
        for reloader in self.reloaders:
            with self.subTest(reloader=reloader.__class__):
                non_py_file = self.tempdir / 'non_py_file'
                non_py_file.touch()
                reloader.watch_dir(self.tempdir, '*.py')
                with self.start_reloader(reloader) as results:
                    self.existing_file.touch()
                self.assertCountEqual(results, [self.existing_file])

    def test_multiple_globs(self):
        for reloader in self.reloaders:
            with self.subTest(reloader=reloader.__class__):
                non_py_file = self.tempdir / 'x.test'
                non_py_file.touch()
                reloader.watch_dir(self.tempdir, '*.py')
                reloader.watch_dir(self.tempdir, '*.test')
                with self.start_reloader(reloader) as results:
                    self.existing_file.touch()
                self.assertCountEqual(results, [self.existing_file])

    def test_overlapping_globs(self):
        for reloader in self.reloaders:
            with self.subTest(reloader=reloader.__class__):
                reloader.watch_dir(self.tempdir, '*.py')
                reloader.watch_dir(self.tempdir, '*.p*')
                with self.start_reloader(reloader) as results:
                    self.existing_file.touch()
                self.assertCountEqual(results, [self.existing_file])

    def test_glob_recursive(self):
        for reloader in self.reloaders:
            with self.subTest(reloader=reloader.__class__):
                subdir = self.tempdir / 'dir'
                subdir.mkdir(exist_ok=True)
                non_py_file = subdir / 'non_py_file'
                non_py_file.touch()
                py_file = subdir / 'file.py'
                py_file.touch()
                reloader.watch_dir(self.tempdir, '*.py', recursive=True)
                with self.start_reloader(reloader) as results:
                    py_file.touch()
                self.assertCountEqual(results, [py_file])

    def test_multiple_recursive_globs(self):
        for reloader in self.reloaders:
            with self.subTest(reloader=reloader.__class__):
                subdir = self.tempdir / 'dir'
                subdir.mkdir(exist_ok=True)
                non_py_file = subdir / 'test.python'
                non_py_file.touch()
                py_file = subdir / 'file.py'
                py_file.touch()
                reloader.watch_dir(self.tempdir, '*.py', recursive=True)
                reloader.watch_dir(self.tempdir, '*.py*', recursive=True)
                with self.start_reloader(reloader) as results:
                    non_py_file.touch()
                    py_file.touch()
                self.assertCountEqual(results, [py_file, non_py_file])

    def test_overlapping_glob_recursive(self):
        for reloader in self.reloaders:
            with self.subTest(reloader=reloader.__class__):
                subdir = self.tempdir / 'dir'
                subdir.mkdir(exist_ok=True)
                py_file = subdir / 'file.py'
                py_file.touch()
                reloader.watch_dir(self.tempdir, '*.py', recursive=True)
                reloader.watch_dir(subdir, '*.py', recursive=True)
                with self.start_reloader(reloader) as results:
                    py_file.touch()
                self.assertCountEqual(results, [py_file])
