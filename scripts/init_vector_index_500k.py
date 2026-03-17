import argparse
import logging
from pathlib import Path

import lancedb
import numpy as np
import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("EXPEDIA.IVFPQ500K")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize incoming shard schema to EXPEDIA table schema."""
    rename_map = {}
    for col in df.columns:
        cl = col.lower()
        if cl == "accessionid":
            rename_map[col] = "id"
        elif cl == "scientificname":
            rename_map[col] = "classification"
        elif cl in ("lineage", "taxonomy"):
            rename_map[col] = "lineage"
        elif cl == "vector":
            rename_map[col] = "vector"
        elif cl == "metagenomic_source":
            rename_map[col] = "metagenomic_source"

    if rename_map:
        df = df.rename(columns=rename_map)

    if "id" not in df.columns:
        raise ValueError("Missing required column 'id'.")
    if "classification" not in df.columns:
        df["classification"] = "Unknown Organism"
    if "lineage" not in df.columns:
        df["lineage"] = "Unclassified"
    if "metagenomic_source" not in df.columns:
        df["metagenomic_source"] = "NCBI-Nucleotide"
    if "vector" not in df.columns:
        raise ValueError("Missing required column 'vector'.")

    # Ensure vectors are float32 lists for LanceDB.
    df["vector"] = df["vector"].apply(lambda v: np.asarray(v, dtype=np.float32).tolist())
    return df[["id", "classification", "lineage", "metagenomic_source", "vector"]]


def build_index(db_path: Path, shards_dir: Path) -> None:
    db_path.mkdir(parents=True, exist_ok=True)
    db = lancedb.connect(str(db_path))
    table_name = "EXPEDIA_ATLAS_500K"

    shard_paths = [shards_dir / f"shard_{i}.parquet" for i in range(1, 6)]
    missing = [str(p) for p in shard_paths if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing shard files: {missing}")

    logger.info("Loading shard_1.parquet and creating table '%s'...", table_name)
    first_df = _normalize_columns(pd.read_parquet(shard_paths[0]))

    if table_name in db.table_names():
        logger.info("Table already exists. Dropping '%s' for clean rebuild.", table_name)
        db.drop_table(table_name)

    table = db.create_table(table_name, data=first_df)

    for shard in shard_paths[1:]:
        logger.info("Appending %s ...", shard.name)
        shard_df = _normalize_columns(pd.read_parquet(shard))
        table.add(shard_df)

    total_rows = len(table)
    logger.info("Consolidation complete. Rows in table: %,d", total_rows)

    logger.info("Creating IVF-PQ index on 'vector' (num_partitions=512, num_sub_vectors=96)...")
    table.create_index(
        metric="cosine",
        vector_column_name="vector",
        index_type="IVF_PQ",
        num_partitions=512,
        num_sub_vectors=96,
        replace=True,
    )

    logger.info("Index build complete for '%s'.", table_name)


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize EXPEDIA 500k LanceDB IVF-PQ index.")
    parser.add_argument(
        "--db-path",
        default=r"E:\EXPEDIA_Data\data\db",
        help="Local LanceDB directory path.",
    )
    parser.add_argument(
        "--shards-dir",
        default=".",
        help="Directory containing shard_1.parquet .. shard_5.parquet.",
    )
    args = parser.parse_args()

    build_index(Path(args.db_path), Path(args.shards_dir))


if __name__ == "__main__":
    main()
