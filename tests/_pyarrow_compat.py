"""Shared PyArrowFileIO Windows compatibility shim for test files.

On Windows, ``urllib.parse.urlparse('C:/path')`` treats 'c' as the URI scheme,
which causes ``PyArrowFileIO.fs_by_scheme()`` to raise "Unrecognized filesystem
type".  This module patches ``PyArrowFileIO.parse_location`` so that bare
Windows drive paths and ``file:///C:/...`` URIs are handled correctly.

On Linux/macOS this is a no-op (guarded by sys.platform != "win32").

Usage in test modules (call once at module level, after importing sys):

    from tests._pyarrow_compat import patch_pyarrow_file_io
    patch_pyarrow_file_io()
"""

from __future__ import annotations

import sys


def patch_pyarrow_file_io() -> None:
    """Fix PyArrowFileIO.parse_location for Windows bare-drive paths (C:/...).

    SqlCatalog's table creation goes through PyArrowFileIO even when the
    read-back path is bypassed.  On Linux/CI this is a no-op.

    Idempotent: multiple callers (e.g. test modules that each import this
    function at module level) apply the patch at most once.  The sentinel
    attribute ``PyArrowFileIO._athleteos_patched`` is set to ``True`` after
    the first successful patch; subsequent calls return immediately without
    re-wrapping the already-patched method.
    """
    if sys.platform != "win32":
        return
    from pyiceberg.io.pyarrow import PyArrowFileIO

    # Idempotency guard: skip if already patched by a previous call.
    if getattr(PyArrowFileIO, "_athleteos_patched", False):
        return

    _orig = PyArrowFileIO.parse_location

    def _patched(location: str):
        from urllib.parse import urlparse as _up

        # Bare Windows path: C:\... or C:/...
        if len(location) >= 2 and location[1] == ":":
            return "file", "", location.replace("\\", "/")
        uri = _up(location)
        # file:///C:/... → path is /C:/... → strip leading slash
        if uri.scheme == "file" and len(uri.path) >= 3 and uri.path[2] == ":":
            return "file", uri.netloc, uri.path[1:]
        return _orig(location)

    PyArrowFileIO.parse_location = staticmethod(_patched)
    # Mark the class as patched so re-entrant calls from other modules are
    # no-ops.  Using a class attribute (not an instance attribute) ensures the
    # sentinel is visible across all import paths.
    PyArrowFileIO._athleteos_patched = True  # type: ignore[attr-defined]
