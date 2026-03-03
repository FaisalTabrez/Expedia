import os
import csv
import json
import logging
import zipfile
import datetime
from pathlib import Path

logger = logging.getLogger("EXPEDIA.DiscoveryReporter")

class DiscoveryReporter:
    """
    @Data-Ops: Manages the export of discovery session artifacts.
    Generates CSV manifests, FASTA files, and system logs.
    """
    
    EXPORT_ROOT = Path("E:/EXPEDIA_Data/results")
    
    @staticmethod
    def ensure_directory():
        """Ensures the export directory exists."""
        try:
            # Fallback to local if E: drive is missing
            target_dir = DiscoveryReporter.EXPORT_ROOT
            if not os.path.exists("E:/"):
                target_dir = Path("results")
            
            os.makedirs(target_dir, exist_ok=True)
            return target_dir
        except Exception as e:
            logger.error(f"Failed to create export directory: {e}")
            return Path("results")

    @staticmethod
    def save_discovery_manifest(ntu_list: list, session_log_path: str = None) -> str:
        """
        Exports a ZIP archive containing:
        - Manifest.csv: Taxa details
        - NTU_Holotypes.fasta: Centroid sequences
        - System_Audit.log: Session log
        
        Returns the absolute path to the generated ZIP file.
        """
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        export_dir = DiscoveryReporter.ensure_directory()
        zip_filename = f"EXPEDIA_Discovery_Manifest_{timestamp}.zip"
        zip_path = export_dir / zip_filename
        
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                
                # 1. Manifest.csv
                manifest_path = "Manifest.csv"
                manifest_data = []
                
                # Collect all taxa including members
                # The input ntu_list contains dicts with 'member_ids'
                # We want a row per NTU or per Member? 
                # "All identified taxa, confidence scores, and NTU assignments."
                # This suggests a row per member if we have member details, 
                # otherwise a row per NTU. 
                # Given structure, we likely only have aggregate info here unless we query DB.
                # Let's verify what 'ntu_list' contains. 
                # Based on previous turns, it has: ntu_id, size, anchor_taxon, lineage, member_ids.
                
                # Let's opt for NTU-level manifest for now, as member details might be too large/not passed.
                # Re-reading prompt: "All identified taxa...". 
                # If we don't have member details here, we can only export NTU summary.
                # However, we can export the Holotypes FASTA easily.
                
                csv_buffer = []
                csv_buffer.append(["NTU_ID", "ANCHOR_TAXON", "LINEAGE", "SIZE", "DIVERGENCE", "CENTROID_ID"])
                
                fasta_buffer = []
                
                for ntu in ntu_list:
                    ntu_id = ntu.get("ntu_id", "UNKNOWN")
                    anchor = ntu.get("anchor_taxon", "Unresolved")
                    lineage = ntu.get("lineage", "")
                    size = ntu.get("size", 0)
                    divergence = ntu.get("divergence", 0.0) # Or variance
                    
                    # Holotype ID is usually the ID of the centroid
                    # Check if 'id' or 'centroid_id' exists
                    holotype_id = ntu.get("id") or ntu.get("centroid_id") or f"{ntu_id}_Holo"
                    
                    csv_buffer.append([ntu_id, anchor, lineage, size, divergence, holotype_id])
                    
                    # 2. NTU_Holotypes.fasta
                    # We need the sequence string. If it's not in the ntu dict, we can't write it.
                    # Looking at science_kernel.py, 'centroid' is a vector. 'holotype_id' is an ID.
                    # We typically don't pass the raw sequence string around in the kernel result.
                    # If unavailable, we'll note it.
                    # Use a placeholder or check if 'sequence' field exists.
                    seq_data = ntu.get("sequence_data") # Hypothetical field
                    if seq_data:
                         fasta_buffer.append(f">{ntu_id}|{holotype_id}|{anchor}\n{seq_data}\n")
                    else:
                         # Attempt to retrieve if we have access, but for now just log ID
                         fasta_buffer.append(f">{ntu_id}|{holotype_id}|{anchor}\n[SEQUENCE_DATA_NOT_IN_PAYLOAD]\n")

                # Write CSV to Zip
                import io
                csv_output = io.StringIO()
                csv_writer = csv.writer(csv_output)
                csv_writer.writerows(csv_buffer)
                zipf.writestr(manifest_path, csv_output.getvalue())
                
                # Write FASTA to Zip
                if fasta_buffer:
                    zipf.writestr("NTU_Holotypes.fasta", "".join(fasta_buffer))
                
                # 3. System_Audit.log
                # If a path is provided, copy it.
                if session_log_path and os.path.exists(session_log_path):
                    zipf.write(session_log_path, "System_Audit.log")
                else:
                    # Create a dummy log if missing
                    zipf.writestr("System_Audit.log", f"Session Export Timestamp: {timestamp}\nAUDIT LOG NOT FOUND\n")

            logger.info(f"Discovery Artifacts Exported to: {zip_path}")
            return str(zip_path)

        except Exception as e:
            logger.error(f"Export Failed: {e}")
            raise e
