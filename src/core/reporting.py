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
            
            if not target_dir.exists():
                os.makedirs(target_dir, exist_ok=True)
            return target_dir
        except Exception as e:
            logger.error(f"Failed to create export directory: {e}")
            return Path("results")

    @staticmethod
    def save_discovery_manifest(ntu_list: list) -> str:
        """
        Exports a consolidated CSV manifest containing:
        - NTU Identifiers
        - Taxonomic Lineage
        - Confidence Scores (Mean & Holotype)
        - Cluster Metrics
        
        Returns the absolute path to the generated CSV file.
        """
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        export_dir = DiscoveryReporter.ensure_directory()
        csv_filename = f"EXPEDIA_Discovery_Manifest_{timestamp}.csv"
        csv_path = export_dir / csv_filename
        
        try:
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Header: Includes Confidence Metrics
                writer.writerow([
                    "NTU_ID", 
                    "ANCHOR_TAXON", 
                    "LINEAGE", 
                    "SIZE", 
                    "DIVERGENCE", 
                    "MEAN_CONFIDENCE",
                    "HOLOTYPE_CONFIDENCE",
                    "CENTROID_ID"
                ])
                
                for ntu in ntu_list:
                    writer.writerow([
                        ntu.get("ntu_id", "UNKNOWN"),
                        ntu.get("anchor_taxon", "Unresolved"),
                        ntu.get("lineage", ""),
                        ntu.get("size", 0),
                        f"{ntu.get('divergence', 0.0):.4f}",
                        f"{ntu.get('mean_confidence', 0.0):.4f}",
                        f"{ntu.get('holotype_confidence', 0.0):.4f}",
                        ntu.get("centroid_id", "N/A")
                    ])
            
            logger.info(f"Discovery Artifacts Exported to: {csv_path}")
            return str(csv_path)

        except Exception as e:
            logger.error(f"Export Failed: {e}")
            raise e
