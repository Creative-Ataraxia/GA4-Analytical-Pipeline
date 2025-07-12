from datetime import datetime

def list_processed_dates(spark, output_prefix: str) -> set:
    """
    List event_date_dt partitions already written under `output_prefix`.
    Expects folders named like output_prefix/event_date_dt=YYYY-MM-DD/.
    Returns a set of ISO date strings (e.g. '2025-07-09').
    """
    hadoop_conf = spark._jsc.hadoopConfiguration()
    Path = spark._jvm.org.apache.hadoop.fs.Path
    path = Path(output_prefix)
    fs = path.getFileSystem(hadoop_conf)

    try:
        statuses = fs.listStatus(path)
    except Exception:
        return set()

    dates = set()
    for status in statuses:
        name = status.getPath().getName()
        if status.isDirectory() and name.startswith("event_date_dt="):
            try:
                _, date_str = name.split("=", 1)
                dates.add(date_str)
            except ValueError:
                continue
    return dates


def list_raw_dates(spark, input_prefix: str) -> set:
    """
    List raw Parquet files under `input_prefix` named events_YYYYMMDD.parquet.
    Returns a set of ISO date strings (e.g. '2025-07-10').
    """
    hadoop_conf = spark._jsc.hadoopConfiguration()
    Path = spark._jvm.org.apache.hadoop.fs.Path
    path = Path(input_prefix)
    fs = path.getFileSystem(hadoop_conf)

    try:
        statuses = fs.listStatus(path)
    except Exception:
        return set()

    dates = set()
    for status in statuses:
        name = status.getPath().getName()
        if name.startswith("events_") and name.endswith(".parquet"):
            part = name[len("events_"):-len(".parquet")]
            try:
                iso = datetime.strptime(part, "%Y%m%d").date().isoformat()
                dates.add(iso)
            except ValueError:
                continue
    return dates
