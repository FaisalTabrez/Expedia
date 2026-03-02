import logging
import pandas as pd
from collections import Counter
from rapidfuzz import process, fuzz
from ..config import app_config

logger = logging.getLogger("EXPEDIA.Taxonomy")

class TaxonomyEngine:
    """
    @Bio-Taxon: Triple-Tier Hybrid Inference Engine.
    Tier 1: Consensus (Vector Neighbors)
    Tier 2: WoRMS Fuzzy Match
    Tier 3: Lineage Expansion (TaxonKit placeholder)
    """

    def __init__(self):
        # Load WoRMS Reference Database
        self.worms_ref_data = set()
        if app_config.WORMS_CSV.exists():
            try:
                df = pd.read_csv(app_config.WORMS_CSV)
                # Normalize and store valid names
                if 'ScientificName' in df.columns:
                    self.worms_ref_data = set(df['ScientificName'].str.lower().str.strip().tolist())
                logger.info(f"WoRMS Oracle Loaded ({len(self.worms_ref_data)} records).")
            except Exception as e:
                logger.warning(f"Failed to load WoRMS CSV: {e}")
        else:
            logger.warning("WoRMS Reference CSV not found.")

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
                "workflow": "Tier 0"
            }

        # 0. Perfect Match Bypass (Identity Matcher)
        top_hit = neighbors_df.iloc[0]
        best_dist = float(top_hit.get('_distance', 1.0)) # Ensure float
        
        # String Normalization Helper
        def _normalize(s):
            return str(s).replace('_', ' ').strip().lower()

        top_name = _normalize(top_hit.get('classification', ''))
        
        # Check Identity Match (< 5% Divergence)
        if best_dist < 0.05:
             # Check WoRMS Promotion
             # If top hit is 'uncultured' but we have a WoRMS match nearby?
             # Or if the top hit *is* a WoRMS name, prefer it.
             # Logic: If top_name is valid (not blacklisted) and in WoRMS, auto-accept.
             
             is_blacklisted = False
             blacklist = ['uncultured', 'unidentified', 'metagenome', 'environmental']
             for b in blacklist:
                 if b in top_name: is_blacklisted = True
             
             final_name = top_hit.get('classification') # Original casing
             
             # WoRMS Override
             if top_name in self.worms_ref_data:
                 status = "Known (WoRMS)"
             elif not is_blacklisted:
                 status = "Known"
             else:
                 # It's a close match to "Uncultured bacterium" - still effectively unknown/novel
                 status = "Ambiguous"

             if status.startswith("Known"):
                 return {
                    "status": "Identified",
                    "classification": final_name,
                    "confidence": 1.0 - best_dist,
                    "lineage": self._build_lineage_string(neighbors_df),
                    "workflow": "Identity Match (Tier 1)"
                }

        # Flow continues to consensus if not a perfect match...
        
        best_sim = 1.0 - best_dist

        # 1. Try Species Consensus First
        species_cons = self._get_consensus_at_rank(neighbors_df, 'species')
        
        # 2. Build Lineage Breadcrumb (from best match or consensus)
        # We try to build a consensus lineage if possible, otherwise take best match's lineage
        lineage_str = self._build_lineage_string(neighbors_df)

        if best_sim < app_config.THRESHOLD_NOVEL:
             # It's a Novel Entity. Try to find highest confident rank.
             # e.g., "Novel Genus in Family Modiolidae"
             rank_name = self._resolve_deep_lineage(neighbors_df)
             
             return {
                "status": "Novel",
                "classification": rank_name,
                "confidence": best_sim,
                "lineage": lineage_str,
                "workflow": "Discovery"
            }

        # If we have a solid species match
        if species_cons['taxon'] != "Unclassified" and species_cons['confidence'] > 0.5:
             return {
                "status": "Identified",
                "classification": species_cons['taxon'],
                "confidence": species_cons['confidence'],
                "lineage": lineage_str,
                "workflow": "Tier 1 (Consensus)"
            }
        
        # Fallback to Genus or Family if species failed but similarity is high
        # (e.g. distinct species but close to known ones)
        genus_cons = self._get_consensus_at_rank(neighbors_df, 'genus')
        if genus_cons['taxon'] != "Unclassified":
             return {
                "status": "Ambiguous",
                "classification": f"Genus {genus_cons['taxon']} (sp. indet)",
                "confidence": genus_cons['confidence'],
                "lineage": lineage_str,
                "workflow": "Tier 1.5 (Genus Level)"
            }

        return {
            "status": "Unidentified",
            "classification": "Unknown",
            "confidence": 0.0,
            "lineage": lineage_str,
            "workflow": "Tier 0"
        }

    def _get_consensus_at_rank(self, df: pd.DataFrame, rank: str) -> dict:
        """
        Performs filtering and majority voting for a specific taxonomic rank.
        """
        # Forbidden terms (case insensitive)
        BLACKLIST = [
            'unknown', 'incertae sedis', 'nan', 'none', 
            'uncultured', 'environmental sample', 'unidentified', 
            'metagenome', 'bacterium', 'eukaryote'
        ]
        
        # Weighted Vote
        # Weight top result double due to vector similarity
        candidates = []
        for i, (_, row) in enumerate(df.iterrows()):
             val = str(row.get(rank, '')).strip()
             if not val: continue
             
             # Blacklist check
             is_valid = True
             val_lower = val.lower()
             for bad_term in BLACKLIST:
                 if bad_term in val_lower:
                     is_valid = False
                     break
            
             if is_valid:
                 candidates.append(val)
                 if i == 0: # Double weight for #1 neighbor
                     candidates.append(val)

        if not candidates:
            return {"taxon": "Unclassified", "confidence": 0.0}
            
        # Weighted Vote
        counts = Counter(candidates)
        most_common = counts.most_common(1)[0]
        confidence = most_common[1] / len(candidates)
        
        return {"taxon": most_common[0], "confidence": confidence}

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
                
        return "Novel Biological Entity (Deep Divergence)"

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
