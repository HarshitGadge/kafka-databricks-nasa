"""Making this package importable on Spark executors.

A pandas UDF is pickled on the driver and unpickled inside a Python worker on
each executor. The worker is a separate process with its own ``sys.path``, so a
``sys.path.insert`` on the driver -- or a notebook's working directory -- does
not make ``gcn_lakehouse`` importable where the UDF actually runs. The symptom
is a ``ModuleNotFoundError`` raised from the worker on the first batch, long
after the job has started.

The durable fix is to install the package on the cluster:

    %pip install git+https://github.com/HarshitGadge/kafka-databricks-nasa

:func:`ship_to_executors` is the fallback for running straight from a cloned
repo, where building a wheel for every edit is not worth it.
"""

from __future__ import annotations

import zipfile
from pathlib import Path
from tempfile import gettempdir

_PACKAGE_ROOT = Path(__file__).resolve().parent


def ship_to_executors(spark, package_root: Path | None = None) -> str:
    """Zip this package and distribute it to executors via ``addPyFile``.

    Returns the path of the archive that was shipped. Safe to call more than
    once per session; Spark ignores a file it has already distributed.
    """
    source = Path(package_root) if package_root else _PACKAGE_ROOT
    archive = Path(gettempdir()) / f"{source.name}.zip"

    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as bundle:
        for path in sorted(source.rglob("*.py")):
            bundle.write(path, Path(source.name) / path.relative_to(source))

    spark.sparkContext.addPyFile(str(archive))
    return str(archive)
