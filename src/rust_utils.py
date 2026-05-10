"""
管理已编译 Rust 扩展的辅助工具。

本模块有意不导入会加载扩展本身的模块；它只检查文件系统状态。
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import sysconfig
import time
import hashlib
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

LOCK_FILE = Path("passivbot-rust/.compile.lock")
LOCK_TIMEOUT = 300  # 秒
LOCK_CHECK_INTERVAL = 2  # 秒
COMPILED_EXTENSION_NAME = "libpassivbot_rust"
PYTHON_MODULE_NAME = "passivbot_rust"
SOURCE_STAMP_SUFFIX = ".rust-src-sha256"


def _extension_suffixes() -> list[str]:
    # 优先使用精确的解释器后缀（如 `.cpython-312-darwin.so`），
    # 但为非标准环境保留广泛的后备方案。
    suffix = sysconfig.get_config_var("EXT_SUFFIX")
    if suffix:
        return [suffix.lstrip(".")]
    return ["so", "pyd", "dll", "dylib", "bundle", "sl"]


def _local_extension_candidates() -> list[Path]:
    exts = _extension_suffixes()
    out: list[Path] = []
    for ext in exts:
        out.extend(Path("src").glob(f"{PYTHON_MODULE_NAME}*.{ext}"))
    return out


def _installed_extension_candidates() -> list[Path]:
    exts = _extension_suffixes()
    out: list[Path] = []
    # `maturin develop` 生成的已安装扩展通常是 `<site-packages>/passivbot_rust.*.so` 中的
    # 直接模块（平台特定），但某些布局可能将其放在包目录下。
    for key in ("platlib", "purelib"):
        root = sysconfig.get_paths().get(key)
        if not root:
            continue
        root_path = Path(root)
        for ext in exts:
            out.extend(root_path.glob(f"{PYTHON_MODULE_NAME}*.{ext}"))
        pkg_dir = root_path / PYTHON_MODULE_NAME
        if not pkg_dir.exists():
            continue
        for ext in exts:
            out.extend(pkg_dir.glob(f"{PYTHON_MODULE_NAME}*.{ext}"))
    # 去重（platlib/purelib 通常匹配）。
    return list(dict.fromkeys(out))


def _target_extension_candidates() -> list[Path]:
    exts = _extension_suffixes()
    return [
        Path("passivbot-rust/target/release") / f"{COMPILED_EXTENSION_NAME}.{ext}".strip(".")
        for ext in exts
    ]


def compiled_extension_paths() -> List[Path]:
    """
    按导入优先顺序返回扩展候选路径。

    运行 `src/*.py` 脚本时，`src/` 通常在 `sys.path` 的首位，因此本地的
    `src/passivbot_rust*.so` 会遮蔽已安装的 site-packages 构建。
    """
    return (
        _local_extension_candidates()
        + _installed_extension_candidates()
        + _target_extension_candidates()
    )


def _import_target_compiled_path() -> Optional[Path]:
    """
    解析当前进程中 Python 会导入的编译产物路径。

    这对直接扩展模块和 `maturin develop` 生成的包布局都必须有效，
    后者中 `find_spec("passivbot_rust")` 解析到 `__init__.py`，
    编译扩展与其并列存放。
    """
    spec = importlib.util.find_spec(PYTHON_MODULE_NAME)
    if spec is None:
        return None

    origin = getattr(spec, "origin", None)
    if origin and origin not in {"built-in", "frozen"}:
        origin_path = Path(origin)
        suffixes = {suffix.lower() for suffix in _extension_suffixes()}
        if any(str(origin_path).lower().endswith(f".{suffix}") for suffix in suffixes):
            return origin_path

    locations = list(getattr(spec, "submodule_search_locations", []) or [])
    if not locations:
        return None

    suffixes = _extension_suffixes()
    for location in locations:
        location_path = Path(location)
        for ext in suffixes:
            matches = [p for p in location_path.glob(f"{PYTHON_MODULE_NAME}*.{ext}") if p.exists()]
            if matches:
                return max(matches, key=lambda p: p.stat().st_mtime)
    return None


def _compiled_path_from_loaded_module() -> Optional[Path]:
    module = sys.modules.get(PYTHON_MODULE_NAME)
    if module is None:
        return None

    module_file = getattr(module, "__file__", None)
    if module_file and module_file not in {"built-in", "frozen"}:
        module_path = Path(module_file)
        suffixes = {suffix.lower() for suffix in _extension_suffixes()}
        if any(str(module_path).lower().endswith(f".{suffix}") for suffix in suffixes):
            return module_path

    locations = list(getattr(module, "__path__", []) or [])
    if not locations:
        return None

    suffixes = _extension_suffixes()
    for location in locations:
        location_path = Path(location)
        for ext in suffixes:
            matches = [p for p in location_path.glob(f"{PYTHON_MODULE_NAME}*.{ext}") if p.exists()]
            if matches:
                return max(matches, key=lambda p: p.stat().st_mtime)
    return None


def preferred_compiled_mtime() -> Optional[float]:
    """
    最可能被导入的扩展产物的修改时间。

    优先级：
    1) `src/passivbot_rust*.so`（运行 `src/*.py` 时遮蔽一切）
    2) 已安装的 site-packages `passivbot_rust/passivbot_rust*.so`
    3) `passivbot-rust/target/release/libpassivbot_rust.*`
    """
    import_target = _import_target_compiled_path()
    if import_target is not None and import_target.exists():
        return import_target.stat().st_mtime
    for group in (
        _local_extension_candidates(),
        _installed_extension_candidates(),
        _target_extension_candidates(),
    ):
        mtimes = [p.stat().st_mtime for p in group if p.exists()]
        if mtimes:
            return max(mtimes)
    return None

def preferred_compiled_path() -> Optional[Path]:
    """
    最可能被导入的扩展产物的路径。

    优先级与 `preferred_compiled_mtime()` 一致。
    """
    import_target = _import_target_compiled_path()
    if import_target is not None and import_target.exists():
        return import_target
    for group in (
        _local_extension_candidates(),
        _installed_extension_candidates(),
        _target_extension_candidates(),
    ):
        existing = [p for p in group if p.exists()]
        if existing:
            return max(existing, key=lambda p: p.stat().st_mtime)
    return None


def sha256_file(path: str | Path | None) -> Optional[str]:
    if path is None:
        return None
    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        return None
    digest = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def collect_runtime_provenance() -> dict:
    """收集当前进程中 Rust 扩展的运行时来源信息（路径、哈希、时间戳等）。"""
    preferred_path = preferred_compiled_path()
    preferred_str = str(preferred_path) if preferred_path is not None else None
    preferred_hash = sha256_file(preferred_str)
    runtime_path = None
    runtime_hash = None
    runtime_mtime = None
    runtime_stamp = None
    module_loaded = False
    module_name = PYTHON_MODULE_NAME
    module = sys.modules.get(module_name)
    if module is not None:
        module_loaded = True
        runtime_compiled = _compiled_path_from_loaded_module()
        runtime_path = str(runtime_compiled) if runtime_compiled is not None else None
        runtime_hash = sha256_file(runtime_path)
        runtime_stamp = read_source_stamp(runtime_compiled) if runtime_compiled is not None else None
        try:
            runtime_mtime = Path(runtime_path).stat().st_mtime if runtime_path else None
        except OSError:
            runtime_mtime = None
    preferred_mtime = None
    try:
        preferred_mtime = Path(preferred_str).stat().st_mtime if preferred_str else None
    except OSError:
        preferred_mtime = None
    return {
        "module_name": module_name,
        "module_loaded": module_loaded,
        "runtime_module_path": runtime_path,
        "runtime_module_sha256": runtime_hash,
        "runtime_module_mtime": runtime_mtime,
        "runtime_module_source_stamp": runtime_stamp,
        "preferred_compiled_path": preferred_str,
        "preferred_compiled_sha256": preferred_hash,
        "preferred_compiled_mtime": preferred_mtime,
        "runtime_matches_preferred": (
            runtime_hash is not None and preferred_hash is not None and runtime_hash == preferred_hash
        ),
        "pid": os.getpid(),
    }


def latest_compiled_mtime(paths: Iterable[Path]) -> Optional[float]:
    mtimes = [p.stat().st_mtime for p in paths if p.exists()]
    return max(mtimes) if mtimes else None


def _tracked_source_files(root: Path = Path("passivbot-rust")) -> list[Path]:
    tracked_files: list[Path] = []
    for file_path in (root / "Cargo.toml", root / "Cargo.lock"):
        if file_path.exists():
            tracked_files.append(file_path)
    for file_path in root.glob("*.rs"):
        if file_path.exists():
            tracked_files.append(file_path)
    src_root = root / "src"
    if src_root.exists():
        tracked_files.extend(path for path in src_root.rglob("*.rs") if path.exists())
    return sorted(set(tracked_files))


def latest_source_mtime(root: Path = Path("passivbot-rust")) -> Optional[float]:
    """
    返回应触发重建的输入文件的最新修改时间。

    注意：
    - 避免扫描 `target/`，因为构建产物可能包含生成的 `.rs` 文件，
      会导致永久性的"过期"检测。
    """
    mtimes: list[float] = []
    for file_path in _tracked_source_files(root):
        try:
            mtimes.append(file_path.stat().st_mtime)
        except OSError:
            continue
    return max(mtimes) if mtimes else None


def source_fingerprint(root: Path = Path("passivbot-rust")) -> Optional[str]:
    tracked_files = _tracked_source_files(root)
    if not tracked_files:
        return None
    digest = hashlib.sha256()
    for path in tracked_files:
        rel = path.relative_to(root)
        digest.update(str(rel).encode("utf-8"))
        digest.update(b"\0")
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                digest.update(chunk)
        digest.update(b"\0")
    return digest.hexdigest()


def is_stale(compiled_mtime: Optional[float], source_mtime: Optional[float]) -> bool:
    if compiled_mtime is None:
        return True
    if source_mtime is None:
        return False
    return compiled_mtime < source_mtime


def source_stamp_path(compiled_path: Path) -> Path:
    return compiled_path.with_name(f"{compiled_path.name}{SOURCE_STAMP_SUFFIX}")


def read_source_stamp(compiled_path: Path) -> Optional[str]:
    stamp_path = source_stamp_path(compiled_path)
    try:
        if stamp_path.exists():
            return stamp_path.read_text(encoding="utf-8").strip() or None
    except OSError:
        return None
    return None


def write_source_stamp(compiled_path: Path, fingerprint: str) -> None:
    stamp_path = source_stamp_path(compiled_path)
    stamp_path.write_text(f"{fingerprint}\n", encoding="utf-8")


def extension_needs_rebuild(
    compiled_path: Optional[Path],
    source_mtime: Optional[float],
    fingerprint: Optional[str],
) -> bool:
    if compiled_path is None or not compiled_path.exists():
        return True
    stamp = read_source_stamp(compiled_path)
    if fingerprint is not None and stamp is not None:
        return stamp != fingerprint
    if is_stale(compiled_path.stat().st_mtime, source_mtime):
        return True
    if fingerprint is None:
        return False
    return stamp != fingerprint


def acquire_lock(lock_file: Path = LOCK_FILE) -> bool:
    """获取编译锁文件，超时后自动移除过期锁。成功返回 True。"""
    import time

    start = time.time()
    while True:
        try:
            if lock_file.exists():
                age = time.time() - lock_file.stat().st_mtime
                if age > LOCK_TIMEOUT:
                    try:
                        lock_file.unlink()
                    except OSError:
                        pass
                else:
                    if time.time() - start > LOCK_TIMEOUT:
                        try:
                            lock_file.unlink()
                        except OSError:
                            pass
                        return True
                    time.sleep(LOCK_CHECK_INTERVAL)
                    continue
            lock_file.parent.mkdir(parents=True, exist_ok=True)
            lock_file.write_text(str(os.getpid()))
            return True
        except OSError:
            return False


def release_lock(lock_file: Path = LOCK_FILE) -> None:
    try:
        if lock_file.exists():
            lock_file.unlink()
    except OSError:
        pass


def stamp_compiled_extensions(fingerprint: Optional[str]) -> None:
    if fingerprint is None:
        return
    for compiled_path in compiled_extension_paths():
        if not compiled_path.exists():
            continue
        try:
            write_source_stamp(compiled_path, fingerprint)
        except OSError:
            continue


def prune_shadowing_local_extensions() -> None:
    """
    当存在已安装的构建时，移除本地 `src/passivbot_rust*.so` 副本。

    标准的运行时产物是 site-packages 中的 editable-install 输出。`src/` 中的本地
    副本扩展在 `src/` 位于 `sys.path` 首位时会遮蔽该构建，这正是本仓库中
    过期扩展混淆的根源。
    """
    installed = [p for p in _installed_extension_candidates() if p.exists()]
    if not installed:
        return
    for local in _local_extension_candidates():
        try:
            if local.exists():
                local.unlink()
            source_stamp_path(local).unlink(missing_ok=True)
        except OSError:
            continue


def recompile_rust() -> bool:
    try:
        start = time.time()
        result = subprocess.run(  # noqa: S603,S607
            ["maturin", "develop", "--release"],
            cwd="passivbot-rust",
            check=True,
            capture_output=True,
            text=True,
        )
        elapsed = time.time() - start
        print(result.stdout)
        print(f"Rust extension rebuild finished in {elapsed:.2f}s")
        stamp_compiled_extensions(source_fingerprint())
        prune_shadowing_local_extensions()
        return True
    except subprocess.CalledProcessError as e:
        print(e.stderr)
        return False
    except Exception as e:
        print(f"Unexpected error during Rust compile: {e}")
        return False


def check_and_maybe_compile(
    *,
    skip: bool = False,
    force: bool = False,
    fail_on_stale: bool = False,
) -> None:
    """
    确保 Rust 扩展存在且为最新版本。

    必须在导入 passivbot_rust 之前调用。
    """
    if skip:
        return

    if "passivbot_rust" in sys.modules:
        # 已在此进程中加载；如果调用方坚持 force/fail，则报错。
        if force or fail_on_stale:
            raise RuntimeError("passivbot_rust is already imported; restart required.")
        print("passivbot_rust already imported; using existing binary.")
        return

    # 优先使用 site-packages 中的 editable-install 产物，并删除遮蔽的本地副本。
    prune_shadowing_local_extensions()

    source_mtime = latest_source_mtime()
    fingerprint = source_fingerprint()
    compiled_path = preferred_compiled_path()
    stale = extension_needs_rebuild(compiled_path, source_mtime, fingerprint)

    needs_compile = force or stale
    if fail_on_stale and stale:
        raise RuntimeError("Rust extension is stale; rebuild required (fail-on-stale enabled).")
    if not needs_compile:
        return

    if compiled_path is None:
        print("Rust extension missing; compiling...")
    elif stale:
        print("Rust extension is stale; recompiling...")
    elif force:
        print("Rust extension rebuild forced; recompiling...")

    if not acquire_lock():
        raise RuntimeError("Failed to acquire Rust compile lock.")
    try:
        if not recompile_rust():
            raise RuntimeError("Rust compilation failed.")
    finally:
        release_lock()

    # 编译后重新检查是否过期
    prune_shadowing_local_extensions()
    compiled_path = preferred_compiled_path()
    if extension_needs_rebuild(compiled_path, latest_source_mtime(), source_fingerprint()):
        raise RuntimeError("Rust extension appears stale even after recompilation; check build.")


def sync_installed_extension_into_src() -> None:
    """
    已弃用的兼容性垫片。

    修复过期扩展遮蔽问题的可靠方法是完全移除本地 `src/passivbot_rust*.so` 副本，
    依赖 site-packages 中已安装的 editable 构建。
    """
    prune_shadowing_local_extensions()


def verify_loaded_runtime_extension(*, fingerprint: Optional[str] = None) -> dict:
    """
    验证此 Python 进程中加载的编译产物是否与当前 Rust 源码匹配。

    应在长期运行的命令入口点中导入 `passivbot_rust` 后调用。
    """
    if fingerprint is None:
        fingerprint = source_fingerprint()

    module = sys.modules.get(PYTHON_MODULE_NAME)
    runtime_path = _compiled_path_from_loaded_module()
    if runtime_path is None and module is not None:
        # 测试环境通常会安装一个轻量级的桩模块。
        if not hasattr(module, "__path__") and not getattr(module, "__file__", None):
            return {
                "runtime_compiled_path": None,
                "runtime_compiled_sha256": None,
                "runtime_compiled_source_stamp": None,
                "expected_source_fingerprint": fingerprint,
                "skipped": "stub_module",
            }
    if runtime_path is None or not runtime_path.exists():
        raise RuntimeError("Loaded Rust extension path could not be resolved.")

    stamp = read_source_stamp(runtime_path)
    if fingerprint is not None and stamp is not None and stamp != fingerprint:
        raise RuntimeError(
            "Loaded Rust extension does not match current Rust sources; restart after rebuild."
        )
    if fingerprint is not None and stamp is None and is_stale(runtime_path.stat().st_mtime, latest_source_mtime()):
        raise RuntimeError(
            "Loaded Rust extension has no source fingerprint stamp and appears stale."
        )

    return {
        "runtime_compiled_path": str(runtime_path),
        "runtime_compiled_sha256": sha256_file(runtime_path),
        "runtime_compiled_source_stamp": stamp,
        "expected_source_fingerprint": fingerprint,
    }
