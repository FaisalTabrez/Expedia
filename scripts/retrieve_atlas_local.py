import os
import time
import shutil
import io
import gc
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from Bio import Entrez, SeqIO

# --- CONFIGURATION ---
Entrez.email = "data-ops@deepbio.scan"  # TODO: Change to your email
TARGET_COUNT = 500_000
SHARD_SIZE = 100_000
FETCH_BATCH_SIZE = 200   # Smaller batch (200) to safely prevent IncompleteRead
SHARDS_DIR = "shards_local"
FORCE_REBUILD = False   # Resume capability: Don't delete good shards
MAX_RETRIES = 12        # Heavy retry logic for DNS/Connection drops
RETRY_BASE_DELAY = 5    # Start retrying sooner (5s, 10s, 20s...)

# Taxonomic Ranks of Interest
RANKS = ["Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "Species"]

def _rank_aware_fill(rank_values):
    """Fills missing ranks with [Unclassified Rank] placeholder."""
    out = {}
    for rank in RANKS:
        val = str(rank_values.get(rank, "")).strip()
        if not val or val.lower() in {"unknown", "nan", "none"}:
            out[rank] = f"[Unclassified {rank}]"
        else:
            out[rank] = val
    return out

def fetch_taxonomy_metadata(tax_ids, retry_count=0):
    """Fetches taxonomy details given a list of TaxIDs."""
    if not tax_ids: return {}
    tax_dict = {}
    try:
        handle = Entrez.efetch(db="taxonomy", id=",".join(tax_ids), retmode="xml")
        records = Entrez.read(handle)
        handle.close()
        for record in records:
            tax_id = str(record.get("TaxId", "Unknown"))
            rank_values = {r: "" for r in RANKS}
            
            # Source 1: Rank explicitly in record
            rec_rank = str(record.get("Rank", "")).strip().lower()
            rec_name = str(record.get("ScientificName", "")).strip()
            if rec_rank in {r.lower() for r in RANKS} and rec_name:
                rank_values[rec_rank.capitalize()] = rec_name
            
            # Source 2: Lineage expansion
            for taxon in record.get("LineageEx", []):
                rank = str(taxon.get("Rank", "")).strip().lower()
                name = str(taxon.get("ScientificName", "")).strip()
                if rank in {r.lower() for r in RANKS} and name:
                    rank_values[rank.capitalize()] = name
            
            tax_dict[tax_id] = _rank_aware_fill(rank_values)
        del records
        return tax_dict
    except Exception as e:
        if retry_count < MAX_RETRIES:
            wait = RETRY_BASE_DELAY * (2 ** retry_count)
            print(f"  [Warn] Taxonomy fetch error: {e}. Retrying in {wait}s...")
            time.sleep(wait)
            return fetch_taxonomy_metadata(tax_ids, retry_count + 1)
        print(f"  [Error] Taxonomy fetch permanently failed: {e}")
    return {}

def fetch_batch_sequences(start, retmax, webenv, query_key, retry_count=0):
    """Fetches sequences in FASTA format."""
    try:
        fasta_handle = Entrez.efetch(
            db="nucleotide", 
            rettype="fasta", 
            retmode="text", 
            retstart=start, 
            retmax=retmax, 
            webenv=webenv, 
            query_key=query_key
        )
        blob = fasta_handle.read()
        fasta_handle.close()
        
        if isinstance(blob, bytes): 
            blob = blob.decode('utf-8', errors='replace')
            
        res = list(SeqIO.parse(io.StringIO(blob), "fasta-pearson"))  # Using fasta-pearson to avoid warnings
        del blob
        return res
    except Exception as e:
        if retry_count < MAX_RETRIES:
            wait = RETRY_BASE_DELAY * (2 ** retry_count)
            print(f"  [Warn] Seq fetch error at {start}: {e}. Retrying in {wait}s...")
            time.sleep(wait)
            return fetch_batch_sequences(start, retmax, webenv, query_key, retry_count + 1)
        print(f"  [Error] Seq fetch failed at {start}: {e}")
        return None

def fetch_batch_metadata(start, retmax, webenv, query_key, retry_count=0):
    """Fetches summary metadata (TaxID, Organism, etc)."""
    try:
        sh = Entrez.esummary(db="nucleotide", retstart=start, retmax=retmax, webenv=webenv, query_key=query_key)
        res = Entrez.read(sh)
        sh.close()
        return res
    except Exception as e:
        if retry_count < MAX_RETRIES:
            wait = RETRY_BASE_DELAY * (2 ** retry_count)
            print(f"  [Warn] Meta fetch error at {start}: {e}. Retrying in {wait}s...")
            time.sleep(wait)
            return fetch_batch_metadata(start, retmax, webenv, query_key, retry_count + 1)
        return None

def process_shard(shard_idx, shard_file, shard_start, shard_end, webenv, query_key, schema):
    """Processes a single shard range."""
    print(f"\nShard {shard_idx+1}: Processing range {shard_start:,} -> {shard_end:,}")
    print(f"Output: {shard_file}")
    
    writer = pq.ParquetWriter(shard_file, schema)
    total_shard_rows = 0
    t0 = time.time()

    # --- Loop ---
    for start in range(shard_start, shard_end, FETCH_BATCH_SIZE):
        rmax = min(FETCH_BATCH_SIZE, shard_end - start)
        
        # 1. Fetch Sequences
        seqs = fetch_batch_sequences(start, rmax, webenv, query_key)
        if not seqs:
            gc.collect()
            continue
        
        # 2. Fetch Metadata
        meta = fetch_batch_metadata(start, rmax, webenv, query_key)
        if not meta: 
            del seqs
            gc.collect()
            continue

        # 3. Build Maps
        acc_map = {}
        for d in meta:
            tid = str(d.get("TaxId", "Unknown"))
            if d.get("AccessionVersion"): acc_map[d["AccessionVersion"].split('.')[0]] = tid
            if d.get("Caption"): acc_map[d["Caption"].split('.')[0]] = tid
        
        tids = list({t for t in acc_map.values() if t != "Unknown"})
        if not tids:
            tax_lookup = {}
        else:
            tax_lookup = fetch_taxonomy_metadata(tids)
        
        # 4. Construct Rows
        rows = []
        for s in seqs:
            seq_str = str(s.seq).upper()
            # Length filter (matches Colab logic)
            if not (200 <= len(seq_str) <= 2000): continue
            
            acc = s.id.split('.')[0]
            tx_id = acc_map.get(acc, "Unknown")
            tx_info = tax_lookup.get(tx_id, _rank_aware_fill({}))
            desc = s.description.split(' ', 1)
            
            rows.append({
                "AccessionID": acc,
                "ScientificName": desc[1] if len(desc)>1 else "Unknown",
                "TaxID": tx_id,
                **tx_info,
                "Sequence": seq_str,
                "metagenomic_source": "NCBI-Nucleotide",
                "Quality_Check": True
            })
        
        # Cleanup
        del seqs, meta, acc_map, tids, tax_lookup
        
        if rows:
            rb = pa.RecordBatch.from_pandas(pd.DataFrame(rows), schema=schema)
            # Write batch immediately
            writer.write_batch(rb)
            
            count = len(rows)
            total_shard_rows += count
            
            elapsed = time.time() - t0
            rate = total_shard_rows / elapsed if elapsed > 0 else 0
            print(f"  Processed {total_shard_rows:,} records ({rate:.1f} rec/s)...", end="\r")
            
            del rows, rb
        
        # GC and Logging
        gc.collect()
        
        # Gentle buffer delay for local network
        time.sleep(0.5)

    writer.close()
    print(f"\n✅ Shard {shard_idx+1} Complete. Records: {total_shard_rows:,}")
    return total_shard_rows

def main():
    if os.path.exists(SHARDS_DIR):
        if FORCE_REBUILD:
            print(f"Cleaning existing directory: {SHARDS_DIR}")
            shutil.rmtree(SHARDS_DIR)
        else:
            print(f"Using existing directory: {SHARDS_DIR}")
    os.makedirs(SHARDS_DIR, exist_ok=True)

    print(f"--- Starting Local Retrieval of {TARGET_COUNT:,} Marine Records ---")
    
    # 1. Search Eukaryotes
    query = "eukaryota[Organism] AND (marine[All Fields] OR ocean[All Fields] OR benthic[All Fields]) AND (18S[All Fields] OR COI[All Fields])"
    print("Searching NCBI...")
    try:
        handle = Entrez.esearch(db="nucleotide", term=query, retmax=TARGET_COUNT, usehistory="y")
        record = Entrez.read(handle)
        handle.close()
    except Exception as e:
        print(f"❌ Search failed: {e}")
        return

    total_found = int(record["Count"])
    webenv = record["WebEnv"]
    query_key = record["QueryKey"]
    print(f"Found {total_found:,} records available.")
    
    fetch_limit = min(TARGET_COUNT, total_found)
    print(f"Fetching {fetch_limit:,} records...")

    # Define Schema
    schema = pa.schema([
        ('AccessionID', pa.string()), ('ScientificName', pa.string()), ('TaxID', pa.string()),
        ('Kingdom', pa.string()), ('Phylum', pa.string()), ('Class', pa.string()),
        ('Order', pa.string()), ('Family', pa.string()), ('Genus', pa.string()),
        ('Species', pa.string()), ('Sequence', pa.string()), 
        ('metagenomic_source', pa.string()), ('Quality_Check', pa.bool_())
    ])

    # Loop Shards
    for shard_idx in range(5):
        shard_start = shard_idx * SHARD_SIZE
        shard_end = min(shard_start + SHARD_SIZE, fetch_limit)
        
        if shard_start >= fetch_limit:
            break
            
        shard_file = os.path.join(SHARDS_DIR, f"shard_{shard_idx + 1}.parquet")

        # Resume Check: Only skip if shard exists AND has substantial data (>50k rows)
        if os.path.exists(shard_file) and not FORCE_REBUILD:
            try:
                pf = pq.ParquetFile(shard_file)
                if pf.metadata.num_rows > 50000:
                    print(f"Skipping completed shard: {shard_file} ({pf.metadata.num_rows:,} records)")
                    continue
                else:
                    print(f"Overwriting incomplete/empty shard: {shard_file} ({pf.metadata.num_rows:,} records)")
                    try:
                        os.remove(shard_file)
                    except:
                        pass
            except Exception as e:
                print(f"Overwriting corrupted shard: {shard_file} ({e})")
                try:
                    os.remove(shard_file)
                except:
                    pass
        
        # process_shard(shard_idx, shard_file, shard_start, shard_end, webenv, query_key, schema)
        # ^ Note: process_shard isn't defined in the provided snippet above, so I'll assume the original logic was inline or a function call.
        # Wait, I see `process_shard` call in the 'original' text I am replacing.
        # I need to implement the call.
        
        # Re-implementing the function call as it was likely defined or I should just use the inline logic if it was inline.
        # Looking at previous `read_file` output (lines 100-120), `process_shard` IS defined.
        
        process_shard(shard_idx, shard_file, shard_start, shard_end, webenv, query_key, schema)
        
        # Pause between shards
        time.sleep(2)


    print("\n🎉 Retrieval Pipeline Complete!")
    print(f"Files saved in: {os.path.abspath(SHARDS_DIR)}")

if __name__ == "__main__":
    main()
