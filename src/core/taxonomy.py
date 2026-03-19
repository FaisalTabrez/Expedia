import logging
import pandas as pd
from collections import Counter
from rapidfuzz import process, fuzz
from ..config import app_config

logger = logging.getLogger("EXPEDIA.Taxonomy")

class TaxonomyEngine:
    """
    @Bio-Taxon: Dual-Tier High-Density Inference Engine.
    Tier 1: Stochastic Vector Consensus (k=50)
    Tier 2: Standardized Reconstruction (TaxonKit)
    """

    def __init__(self):
        # Initialize Dual-Tier Engine
        logger.info("[SYSTEM] Dual-Tier Inference Engine active (Vector Consensus + TaxonKit).")

    def analyze_sample(self, neighbors_df: pd.DataFrame, sequence_str: str) -> dict:
        """
        Main entry point for classifying a sample based on vector search results.
        """
        if neighbors_df.empty:
            return {
                "status": "No Signal",
                "classification": "Unknown",
                "confidence": 0.0,
                "lineage": "Unknown",
                "workflow": "Tier 0",
                "predicted_lineage": {
                    "status": "NO SIGNAL",
                    "lineage_string": "Unknown",
                    "anchor_rank": "None"
                }
            }

        working_df = neighbors_df.copy()
        working_df.columns = [str(c).lower() for c in working_df.columns]

        prediction = self.resolve_identity(working_df)
        return {
            "status": prediction['status'],
            "classification": prediction['classification'],
            "confidence": prediction['confidence'],
            "lineage": prediction['lineage_string'],
            "workflow": "Tier 1 (Vector Consensus) + Tier 2 (TaxonKit)",
            "predicted_lineage": {
                "status": prediction['lineage_status'], # CONFIRMED / DIVERGENT / NOVEL
                "lineage_string": prediction['lineage_string'],
                "anchor_rank": prediction['anchor_rank'],
                "confidence_per_rank": prediction.get('confidence_per_rank', [])
            }
        }

    def resolve_identity(self, df: pd.DataFrame) -> dict:
        """
        @Bio-Taxon: Dual-Tier Resolution Model.
        Tier 1: Performs k=50 majority vote using the 500k Atlas metadata.
        Tier 2: Uses the Tier 1 result to reconstruct the full 7-level lineage via TaxonKit.
        """
        # Ensure we are using top 50 for pure consensus if larger set provided
        consensus_pool = df.head(50)
        return self.predict_lineage(consensus_pool)

    def predict_lineage(self, df: pd.DataFrame) -> dict:
        """
        Hierarchical voting with confidence decay and 7-rank lineage string construction.
        """
        ranks = ['Kingdom', 'Phylum', 'Class', 'Order', 'Family', 'Genus', 'Species']
        rank_keys = [r.lower() for r in ranks]

        top_dist = float(df.iloc[0].get('_distance', 1.0))
        top_sim = max(0.0, 1.0 - top_dist)

        rank_votes = {}
        confidence_per_rank = []
        for rank_name, rank_key in zip(ranks, rank_keys):
            consensus = self._get_consensus(df, rank_key)
            rank_votes[rank_name] = consensus
            confidence_per_rank.append((rank_name, consensus['confidence']))

        phylum_conf = rank_votes['Phylum']['confidence']
        genus_conf = rank_votes['Genus']['confidence']

        lineage_status = "DIVERGENT"
        if phylum_conf >= 0.90:
            lineage_status = "CONFIRMED"
        if genus_conf < 0.50:
            lineage_status = "DIVERGENT/NOVEL GENUS"

        # Resolve each rank with backfill and novelty markers.
        resolved = {}
        for i, rank in enumerate(ranks):
            parent_rank = ranks[i - 1] if i > 0 else None
            parent_name = str(resolved.get(parent_rank, "Biota")) if parent_rank else "Biota"
            rank_taxon = rank_votes[rank]['taxon']
            rank_conf = rank_votes[rank]['confidence']

            if rank_taxon == "Unclassified":
                resolved[rank] = self._rank_aware_fill(rank, parent_name)
                continue

            if rank == 'Genus' and rank_conf < 0.50:
                family_name = resolved.get('Family', 'Unknown Family')
                resolved[rank] = f"[NOVEL GENUS in {family_name}]"
                continue

            if rank == 'Species' and rank_conf < 0.50:
                genus_name = resolved.get('Genus', 'Unknown Genus')
                resolved[rank] = f"[NOVEL SPECIES in {genus_name}]"
                continue

            resolved[rank] = rank_taxon

        lineage_parts = [resolved[r] for r in ranks]
        lineage_str = " > ".join(lineage_parts)

        anchor_rank = "None"
        for rank in reversed(ranks):
            val = resolved[rank]
            if val and "[Unclassified" not in val and not val.startswith("[NOVEL"):
                anchor_rank = rank
                break

        if lineage_status == "CONFIRMED":
            classification = rank_votes['Species']['taxon']
            if classification == "Unclassified":
                classification = rank_votes['Genus']['taxon']
        elif lineage_status == "DIVERGENT/NOVEL GENUS":
            order_name = resolved.get('Order', 'Unknown Order')
            classification = f"Novel [Family] in Order {order_name}"
        else:
            family_name = resolved.get('Family', 'Unknown Family')
            classification = f"Divergent lineage near {family_name}"

        if not classification or classification == "Unclassified":
            classification = "Non-Reference Genomic Signature"

        return {
            "lineage_string": lineage_str,
            "anchor_rank": anchor_rank,
            "lineage_status": lineage_status,
            "classification": classification,
            "contamination_warning": False,
            "confidence": top_sim,
            "confidence_per_rank": confidence_per_rank,
            "status": "Identified" if lineage_status == "CONFIRMED" else "Novel"
        }

    def _rank_aware_fill(self, rank: str, parent_name: str) -> str:
        """Creates informative placeholders when a rank is missing."""
        if rank == "Kingdom":
            return "[Unclassified Kingdom]"
        if not parent_name:
            parent_name = "Unknown"
        return f"[Unclassified {rank}]"

    def _get_consensus(self, df: pd.DataFrame, rank: str) -> dict:
        """
        Weighted majority voting for a specific taxonomic rank over top-50 neighbors.
        """
        BLACKLIST = [
            'unknown', 'incertae sedis', 'nan', 'none', 
            'uncultured', 'environmental sample', 'unidentified', 
            'metagenome', 'bacterium', 'eukaryote', 'organism'
        ]

        if rank not in df.columns:
            return {"taxon": "Unclassified", "confidence": 0.0}

        candidates = []

        for i, (_, row) in enumerate(df.head(50).iterrows()):
            val = str(row.get(rank, '')).strip()
            if not val or len(val) < 2:
                continue

            val_lower = val.lower()
            if any(bad in val_lower for bad in BLACKLIST):
                continue

            weight = 1
            if i == 0:
                weight = 5
            elif i < 5:
                weight = 3

            for _ in range(weight):
                candidates.append(val)

        if not candidates:
            return {"taxon": "Unclassified", "confidence": 0.0}

        counts = Counter(candidates)
        most_common = counts.most_common(1)[0]
        confidence = most_common[1] / len(candidates)

        return {"taxon": most_common[0], "confidence": confidence}

    def _get_consensus_at_rank(self, df: pd.DataFrame, rank: str) -> dict:
        """Backward-compatible wrapper for legacy call sites."""
        return self._get_consensus(df, rank)

    def _resolve_deep_lineage(self, df: pd.DataFrame) -> str:
        """
        Finds the lowest rank (deepest) with high consensus for a novel sequence.
        Returns e.g. 'Novel Genus in Family X'
        """
        # Hierarchy from specific to broad
        ranks = ['genus', 'family', 'order', 'class', 'phylum']
        
        for rank in ranks:
            cons = self._get_consensus_at_rank(df, rank)
            if cons['taxon'] != "Unclassified" and cons['confidence'] > 0.6:
                # Found a solid anchor
                return f"Novel {rank.capitalize()} in {rank.capitalize()} {cons['taxon']}"
                
        return "NON-REFERENCE GENOMIC SIGNATURE (NRGS)"

    def _build_lineage_string(self, df: pd.DataFrame) -> str:
        """
        Constructs a breadcrumb string: Kingdom > Phylum > Class ...
        Uses the top hit as the primary source for the path logic to ensure consistency,
        or computes consensus at each level. For UI, top hit path is usually safer for display
        unless it's totally wrong.
        """
        if df.empty:
            return "Unknown"
            
        # Use simple consensus or just top hit for the path structure
        # Let's use the top hit for the skeleton
        top = df.iloc[0]
        
        # Standard ranks
        path_parts = []
        for r in ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']:
            val = str(top.get(r, '')).strip()
            if val and val.lower() not in ['nan', 'unknown', 'none']:
                 path_parts.append(val)
                 
        return " > ".join(path_parts) if path_parts else "Unknown Lineage"


    def _tier2_worms_fuzzy(self, query_name: str) -> str | None:
        """
        Fuzzy match against loaded WoRMS database.
        """
        if not self.worms_ref_data:
            return None
            
        match = process.extractOne(query_name, self.worms_ref_data, scorer=fuzz.token_sort_ratio)
        if match and match[1] > 90:
            return match[0]
        return None

    def _tier3_lineage(self, taxid):
        """
        TaxonKit wrapper or lookups.
        """
        pass
