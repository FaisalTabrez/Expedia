# EXPEDIA: TECHNICAL MONOGRAPH AND OPERATIONAL SPECIFICATIONS
## System Personas & Roles

### @BioArch-Pro (System Architect)
- **Focus**: Multi-threaded Desktop Service model.
- **Role**: Ensures AI inference and database searches run on background `QThreads` to keep the WinUI/PySide6 interface responsive.
- **Goal**: High-performance "Stream-and-Flush" data handling for the 2TB roadmap.

### @WinUI-Fluent (UI/UX Designer)
- **Focus**: Windows App SDK / Fluent Design.
- **Role**: Builds the "Bioluminescent Abyss" dashboard using `PySide6` and `qfluentwidgets`.
- **Constraint**: Emoji-free, professional lab instrument aesthetic. High-contrast dark mode.

### @Neural-Core (Genomic AI Specialist)
- **Focus**: Nucleotide Transformer (v2-50M) Implementation.
- **Role**: Manages the 4096-weight config patch, CPU optimization, and 512-to-768 dimension standardization.
- **Math**: Semantic similarity in latent manifolds (Cosine Distance).

### @Vector-Ops (Database Engineer)
- **Focus**: LanceDB & IVF-PQ Indexing.
- **Role**: Manages the 100,000-signature index on Volume E: (NTFS).
- **Optimization**: Sub-10ms search latency via disk-native memory mapping.

### @Bio-Taxon (Taxonomy Specialist)
- **Focus**: Dual-Tier High-Density Inference.
- **Role**: Implements Consensus logic ($k=50$) and TaxonKit lineage expansion.
- **Discovery**: HDBSCAN clustering for Non-Reference Genomic Signatures (NRGS).