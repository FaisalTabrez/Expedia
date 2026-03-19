# EXPEDIA: Genomic Surveillance System

**Deep-Sea Semantic Biodiversity Discovery Engine (WinUI 3)**

## Abstract
**EXPEDIA** (Ecological X-PLoration & Evolutionary Discovery via AI) is a professional, standalone Windows application architected for real-time deep-sea eDNA biodiversity discovery. Marking a paradigm shift from syntactic alignment (BLAST) to semantic latent-space search, EXPEDIA leverages high-dimensional genomic embeddings to identify and characterize widely divergent biological entities in resource-constrained, air-gapped environments.

---

## Core Scientific Features

### 1. Genomic Foundation Model
The system is built upon the **Nucleotide Transformer v2-50M**, a transformer-based language model pre-trained on multispecies genomic sequences.
*   **Dimensionality**: Projects raw nucleotide sequences (A, C, G, T) into a dense **768-dimensional vector space**.
*   **Semantic Understanding**: Captures evolutionary context and functional potential beyond simple sequence identity.

### 2. Avalanche Discovery Standard
EXPEDIA implements a rigorous, non-linear manifold learning pipeline to separate biological signal from noise and identify Non-Reference Taxa (NRT):
1.  **L2 Normalization**: Standardizes vector magnitude to ensure cosine similarity validity.
2.  **Topological Projection (UMAP 10D)**: Reduces dimensionality while preserving local manifold structure ($k=15$, adaptive initialization).
3.  **Density Clustering (HDBSCAN)**: Detects high-density clusters of novel signatures that defy standard classification taxonomies.

### 3. Dual-Tier High-Density Inference
Taxonomic assignment follows a high-density consensus protocol:
*   **Tier 1: Stochastic Vector Consensus ($k=50$)**: Leveraging the 500,000-signature manifold for phylogenetic anchoring.
*   **Tier 2: Standardized Reconstruction (TaxonKit)**: Mapping consensus results to global NCBI metadata standards.

---

## Engineering Excellence

### Service-Oriented Architecture
The application employs a **Process-Isolation Model** to ensure UI responsiveness during heavy computation:
*   **WinUI Display Kernel**: Managing the fluent interface, visualization rendering, and user interaction.
*   **Science Kernel**: A dedicated background process executing PyTorch inference and LanceDB queries.
*   **Inter-Process Communication (IPC)**: kernels communicate via asynchronous JSON-RPC over standard input/output streams.

### Air-Gapped Reliability
Designed for deployment on research vessels (e.g., *R/V Atlantis*) operating without internet connectivity:
*   **Frozen Weights**: All AI models are locally cached and version-locked.
*   **Offline Database**: The Reference Atlas and Taxonomy Databases are fully self-contained on the local filesystem.
*   **Embedded Visualization**: Plotly charts are generated as temporary local HTML files, eliminating CDN dependencies.

### Disk-Native Intelligence
Optimized for high-throughput interrogation of massive datasets on standard edge hardware:
*   **LanceDB Integration**: A columnar vector database capable of indexing **100,000+ signatures**.
*   **Zero-Copy Access**: Retrieves vectors directly from NVMe storage (NTFS), achieving **sub-10ms search latency**.

---

## Installation & Deployment

### Hardware Anchor: (NTFS)
The system requires a dedicated high-speed data volume mapped to a dedicated storage device. This ensures consistent absolute path resolution for the Reference Atlas and Model Weights.

### Mandatory Auxiliaries
Ensure the following components are present in the `bin` and `resources` directories:
*   **`taxonkit.exe`**: For rapid lineage processing.
*   **`worms_deepsea_ref.csv`**: The authoritative taxonomic backbone.
*   **`nt_v2_50m`**: The ONNX/PyTorch model weights folder.

### Bootloader
Launch the application using the standardized batch script:
```cmd
Launch_BioScan_Source.bat
```
This script initializes the Python environment, validates dependencies, and spawns the dual-kernel process architecture.

---

## Navigation Guide

The application interface is divided into five specialized operational modules:

1.  **MONITOR**: Real-time dashboard displaying system health, memory usage, and active inference throughput.
2.  **MANIFOLD**: Interactive 3D visualization of the genomic latent space, allowing users to explore relationships between reference and novel taxa.
3.  **INFERENCE**: The primary workspace for loading FASTA files, configuring batch parameters, and executing the classification pipeline.
4.  **DISCOVERY**: A specialized view for analyzing Non-Reference Taxa (NRT), featuring the "Bioluminescent Abyss" Sunburst chart and Novel Taxon Unit (NTU) cards.
5.  **BENCHMARKING**: Tools for assessing model performance, latency testing, and validating system integrity against ground-truth datasets.

---

## The 2TB Roadmap

The current 32GB prototype represents Phase 1 of the EXPEDIA trajectory. The architecture is designed to scale linearly to support the **2TB Taxonomic Reference Catalog (TRC)**.

*   **Scalability**: The LanceDB backend supports out-of-core indexing, allowing the database to grow beyond available RAM.
*   **Metagenomic Arrays**: Future iterations will support direct ingestion of raw metagenomic reads, utilizing the same embedding pipeline to deconstruct complex environmental samples into constituent genomic signals.
*   **Distributed Inference**: The Service-Oriented Architecture is ready to support distributed "Science Kernels" across a local compute cluster for massive parallel processing.
