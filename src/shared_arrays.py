"""
通过 multiprocessing.shared_memory 在进程间共享 NumPy 数组的工具函数。
"""

from __future__ import annotations

from dataclasses import dataclass
from multiprocessing import shared_memory
from typing import Dict, Iterable, Tuple

import numpy as np


@dataclass(frozen=True)
class SharedArraySpec:
    """
    用于附加到共享内存支持的 NumPy 数组的轻量级描述符。
    """

    name: str
    shape: Tuple[int, ...]
    dtype: str


class SharedArrayAttachment:
    """
    已附加共享内存块的 RAII 包装器。
    """

    def __init__(self, spec: SharedArraySpec):
        self.spec = spec
        self._shm = shared_memory.SharedMemory(name=spec.name)
        self.array = np.ndarray(spec.shape, dtype=np.dtype(spec.dtype), buffer=self._shm.buf)

    def close(self) -> None:
        self._shm.close()


class SharedArrayManager:
    """
    管理父进程拥有的共享内存分配。
    """

    def __init__(self) -> None:
        self._owned_blocks: Dict[str, shared_memory.SharedMemory] = {}
        self._arrays: Dict[str, np.ndarray] = {}

    def create_from(self, array: np.ndarray) -> Tuple[SharedArraySpec, np.ndarray]:
        """
        分配与 `array` 大小匹配的共享内存，将数据复制到其中，
        并返回描述符和由共享段支持的 NumPy 视图。
        """
        contiguous = np.ascontiguousarray(array)
        shm = shared_memory.SharedMemory(create=True, size=contiguous.nbytes)
        view = np.ndarray(contiguous.shape, dtype=contiguous.dtype, buffer=shm.buf)
        np.copyto(view, contiguous)
        spec = SharedArraySpec(name=shm.name, shape=contiguous.shape, dtype=contiguous.dtype.str)
        self._owned_blocks[spec.name] = shm
        self._arrays[spec.name] = view
        return spec, view

    def view(self, spec: SharedArraySpec) -> np.ndarray:
        """
        返回此管理器拥有的 spec 对应的 NumPy 视图。
        """
        return self._arrays[spec.name]

    def cleanup(self, specs: Iterable[SharedArraySpec] | None = None) -> None:
        """
        关闭并释放所有拥有的共享内存段。可选择限制为子集。
        """
        to_cleanup = (
            specs
            if specs is not None
            else [
                SharedArraySpec(name, array.shape, array.dtype.str)
                for name, array in self._arrays.items()
            ]
        )
        names = {spec.name for spec in to_cleanup}
        for name in list(self._owned_blocks.keys()):
            if name in names:
                shm = self._owned_blocks.pop(name)
                shm.close()
                shm.unlink()
                self._arrays.pop(name, None)


def attach_shared_array(spec: SharedArraySpec) -> SharedArrayAttachment:
    """
    附加到 `spec` 描述的现有共享内存支持的数组。
    """
    return SharedArrayAttachment(spec)
