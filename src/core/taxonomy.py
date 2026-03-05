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
        
        # Check Identity Match (< 5% Divergence)
        if best_dist < 0.05:
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
                        "anchor_rank": "Species"
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
                "anchor_rank": prediction['anchor_rank']
            }
        }

    def predict_lineage(self, df: pd.DataFrame) -> dict:
        """
        @Bio-Taxon: Hierarchical Consensus Algorithm.
        """
        ranks = ['Kingdom', 'Phylum', 'Class', 'Order', 'Family', 'Genus']
        consensus_path = []
        anchor_rank = "None"
        max_confidence = 0.0
        
        # We need to determine if it's Novel, Divergent, or Confirmed based on consensus
        lineage_status = "NOVEL" 

        for rank in ranks:
            # Get consensus for this rank
            cons = self._get_consensus_at_rank(df, rank.lower())
            taxon = cons['taxon']
            conf = cons['confidence']
            
            # Formatting logic for UI
            # If high confidence (>70%), it's a solid node
            if conf > 0.7:
                consensus_path.append(taxon)
                anchor_rank = f"{rank}: {taxon} ({int(conf*100)}%)"
            elif conf > 0.3:
                 # Inferred/Probable but not confirmed - wrap in brackets
                consensus_path.append(f"[{taxon}]")
            else:
                # Too uncertain, stop propagation or mark as unknown ??
                # The user requirement says: "If a rank is inferred via consensus but not confirmed by identity..."
                # Use cutoff
                pass

        # Build Lineage String
        lineage_str = " > ".join(consensus_path) if consensus_path else "Unresolved Lineage"
        
        # Determine Classification name from the lowest solid rank
        # If we have brackets at the end, the classification is likely "Novel [Genus]"
        last_solid = "Unknown"
        for p in reversed(consensus_path):
            if not p.startswith("["):
                last_solid = p
                break
        
        # Simple heuristic for status
        # If top hit distance > 0.15 (85% sim) -> Novel
        # We need the distance here, but strict separation... let's check input df headers
        top_dist = float(df.iloc[0].get('_distance', 1.0))
        if top_dist < 0.05:
            lineage_status = "CONFIRMED"
            classification = df.iloc[0].get('classification', 'Unknown')
        elif top_dist < 0.20:
             lineage_status = "DIVERGENT"
             classification = f"cf. {last_solid}"
        else:
             lineage_status = "NOVEL"
             classification = f"Novel {last_solid}-like"

        return {
            "lineage_string": lineage_str,
            "anchor_rank": anchor_rank,
            "lineage_status": lineage_status,
            "classification": classification,
            "confidence": 1.0 - top_dist,
            "status": "Identified" if lineage_status == "CONFIRMED" else "Novel"
        }

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
