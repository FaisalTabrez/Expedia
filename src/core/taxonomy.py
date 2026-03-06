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
                "workflow": "Tier 0",
                "predicted_lineage": {
                    "status": "NO SIGNAL",
                    "lineage_string": "Unknown",
                    "anchor_rank": "None"
                }
            }

        # Run Prediction Engine (Hierarchical Consensus)
        prediction = self.predict_lineage(neighbors_df)

        # 0. Perfect Match Bypass (Identity Matcher)
        top_hit = neighbors_df.iloc[0]
        best_dist = float(top_hit.get('_distance', 1.0))
        
        # String Normalization Helper
        def _normalize(s):
            return str(s).replace('_', ' ').strip().lower()

        top_name = _normalize(top_hit.get('classification', ''))
        
        # Check Identity Match (Strict > 97% for Identification, matching new logic)
        if best_dist < 0.03:
             is_blacklisted = False
             blacklist = ['uncultured', 'unidentified', 'metagenome', 'environmental']
             for b in blacklist:
                 if b in top_name: is_blacklisted = True
             
             final_name = top_hit.get('classification') # Original casing
             
             # WoRMS Override
             if top_name in self.worms_ref_data:
                 status_str = "Known (WoRMS)"
             elif not is_blacklisted:
                 status_str = "Known"
             else:
                 status_str = "Ambiguous"

             if status_str.startswith("Known"):
                 return {
                    "status": "Identified",
                    "classification": final_name,
                    "confidence": 1.0 - best_dist,
                    "lineage": prediction['lineage_string'],
                    "workflow": "Identity Match (Tier 1)",
                    "predicted_lineage": {
                        "status": "CONFIRMED",
                        "lineage_string": prediction['lineage_string'],
                        "anchor_rank": "Species",
                        "confidence_per_rank": prediction.get('confidence_per_rank', [])
                    }
                }

        # Use the prediction engine's output for non-perfect matches
        return {
            "status": prediction['status'],
            "classification": prediction['classification'],
            "confidence": prediction['confidence'],
            "lineage": prediction['lineage_string'],
            "workflow": "Tier 1 (Consensus)",
            "predicted_lineage": {
                "status": prediction['lineage_status'], # CONFIRMED / DIVERGENT / NOVEL
                "lineage_string": prediction['lineage_string'],
                "anchor_rank": prediction['anchor_rank'],
                "confidence_per_rank": prediction.get('confidence_per_rank', [])
            }
        }

    def predict_lineage(self, df: pd.DataFrame) -> dict:
        """
        @Bio-Taxon: Rank-Specific Divergence Model.
        Replaces flat thresholds with biological consensus + vector similarity sanity checks.
        """
        # Rank-Specific Biological Thresholds (Similarity)
        RANK_THRESHOLDS = {
            'Species': 0.97,
            'Genus': 0.93,
            'Family': 0.88,
            'Order': 0.75,
            'Class': 0.70,
            'Phylum': 0.60,
            'Kingdom': 0.50
        }
        
        # Taxonomy Hierarchy (Top-Down)
        ranks = ['Kingdom', 'Phylum', 'Class', 'Order', 'Family', 'Genus', 'Species']
        
        # Get Top Hit Similarity (1.0 - distance)
        top_dist = float(df.iloc[0].get('_distance', 1.0))
        top_sim = 1.0 - top_dist
        
        consensus_path = []
        anchor_rank = "None"
        confidence_per_rank = []
        
        # 1. Hierarchical Consensus with Sanity Check
        for rank in ranks:
            # Calculate Consensus (Vote %)
            cons = self._get_consensus_at_rank(df, rank.lower())
            taxon = cons['taxon']
            conf = cons['confidence']
            
            if taxon == "Unclassified":
                continue

            # Biological Sanity Check
            # Even if 100% of neighbors say "Genus X", if we only have 85% similarity,
            # we CANNOT biologically confirm Genus X. It must be bracketed [X] or treated as 'Novel X-like'.
            
            threshold = RANK_THRESHOLDS.get(rank, 0.0)
            is_biologically_supported = top_sim >= threshold
            
            # Logic:
            # - High Consensus (>70%) + Supported Sim -> Confirmed Rank (Normal string)
            # - High Consensus (>70%) + Unsupported Sim -> Inferred/Novel (Bracketed [Name])
            # - Medium Consensus (30-70%) -> Inferred (Bracketed [Name])
            # - Low Consensus (<30%) -> Drop
            
            display_name = taxon
            
            if conf > 0.7:
                if is_biologically_supported:
                    # Confirmed
                    consensus_path.append(taxon)
                    anchor_rank = f"{rank}: {taxon} ({int(conf*100)}%)"
                else:
                    # High consensus but vector distance implies novelty/divergence
                    consensus_path.append(f"[{taxon}]")
                    # We keep previous anchor_rank as valid anchor
            elif conf > 0.3:
                # Weak consensus
                consensus_path.append(f"[{taxon}]")
            
            confidence_per_rank.append((rank, conf))

        # 2. Status Determination (Scientific Labeling)
        # > 97%: IDENTIFIED
        # 93 - 97%: DIVERGENT SPECIES
        # 88 - 93%: NOVEL GENUS
        # 75 - 88%: NOVEL FAMILY/ORDER
        # < 75%: EXTREME NOVELTY (DARK TAXA)
        
        if top_sim > 0.97:
            lineage_status = "IDENTIFIED"
            classification = df.iloc[0].get('classification', 'Unknown')
        elif top_sim >= 0.93:
            lineage_status = "DIVERGENT SPECIES"
            # Name: "Genus sp. (cf. Closest)"
            genus_name = self._get_safe_name(consensus_path, 'Genus')
            classification = f"{genus_name} sp."
        elif top_sim >= 0.88:
            lineage_status = "NOVEL GENUS"
            family_name = self._get_safe_name(consensus_path, 'Family')
            classification = f"Novel Genus ({family_name})"
        elif top_sim >= 0.75:
            lineage_status = "NOVEL FAMILY/ORDER"
            classification = "Novel Order/Family"
        else:
            lineage_status = "EXTREME NOVELTY (DARK TAXA)"
            classification = "Unknown Biological Entity"

        # Build Lineage String
        lineage_str = " > ".join(consensus_path) if consensus_path else "Unresolved Lineage"

        return {
            "lineage_string": lineage_str,
            "anchor_rank": anchor_rank,
            "lineage_status": lineage_status,
            "classification": classification,
            "confidence": top_sim,
            "confidence_per_rank": confidence_per_rank,
            "status": "Identified" if lineage_status == "IDENTIFIED" else "Novel"
        }

    def _get_safe_name(self, path, rank_name):
        """Helper to extract unbracketed name if possible"""
        # This assumes path order matches ranks list somewhat, 
        # but path only contains names. Simplistic extraction:
        # Just grab the last non-bracketed item?
        for p in reversed(path):
            if not p.startswith("["):
                return p
        return "Unknown"

    def _get_consensus_at_rank(self, df: pd.DataFrame, rank: str) -> dict:
        """
        Performs filtering and majority voting for a specific taxonomic rank.
        """
        # Forbidden terms (case insensitive)
        BLACKLIST = [
            'unknown', 'incertae sedis', 'nan', 'none', 
            'uncultured', 'environmental sample', 'unidentified', 
            'metagenome', 'bacterium', 'eukaryote', 'organism'
        ]
        
        candidates = []
        
        # We only look at top 50 (df is usually top 50)
        # Weighted Vote: Top 1 = 5 votes, Top 2-5 = 3 votes, Rest = 1 vote
        for i, (_, row) in enumerate(df.iterrows()):
             val = str(row.get(rank, '')).strip()
             if not val: continue
             
             # Blacklist check
             is_valid = True
             val_lower = val.lower()
             
             if len(val) < 3: is_valid = False # Too short

             for bad_term in BLACKLIST:
                 if bad_term in val_lower:
                     is_valid = False
                     break
            
             if is_valid:
                 # WoRMS Validation (Tier 2) - Validation Check
                 # If we had a full oracle, we'd check here. 
                 # For now, simplistic acceptance.
                 
                 weight = 1
                 if i == 0: weight = 5
                 elif i < 5: weight = 3
                 
                 for _ in range(weight):
                     candidates.append(val)

        if not candidates:
            return {"taxon": "Unclassified", "confidence": 0.0}
            
        # Weighted Vote
        counts = Counter(candidates)
        most_common = counts.most_common(1)[0]
        
        # Confidence = Votes for Winner / Total Votes
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
