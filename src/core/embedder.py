import sys
import torch
import numpy as np
import logging
from transformers import AutoModelForMaskedLM, AutoTokenizer, AutoConfig
from ..config import app_config

# -----------------------------------------------------------------------------
# TRANSFORMERS COMPATIBILITY SHIM (v4.30+ Refactor Fix)
# -----------------------------------------------------------------------------
# Nucleotide Transformer (v2) custom modeling code expects pruning utils in 
# transformers.pytorch_utils, but they were moved/removed in newer versions.
try:
    import transformers
    # Inject missing attribute into the base class to support legacy custom models
    # Fix: User suggested set(), but transformers calls .keys(), so we use {} (dict)
    transformers.PreTrainedModel.all_tied_weights_keys = {} # type: ignore

    # Fix: Inject get_head_mask which was removed in recent transformers but used by EsmModel
    if not hasattr(transformers.PreTrainedModel, "get_head_mask"):
        def get_head_mask(self, head_mask, num_hidden_layers, is_attention_chunked=False):
            return [None] * num_hidden_layers
        transformers.PreTrainedModel.get_head_mask = get_head_mask # type: ignore

    import transformers.pytorch_utils
    import torch
    from torch import nn

    def finding_pruneable_heads_and_indices_shim(heads, n_heads, head_size, already_pruned_heads):
        """
        Finds the heads and their indices taking already_pruned_heads into account.
        """
        mask = torch.ones(n_heads, head_size)
        heads = set(heads) - set(already_pruned_heads)
        for head in heads:
            head = head - sum(1 if h < head else 0 for h in already_pruned_heads)
            mask[head] = 0
        mask = mask.view(-1).eq(1)
        index = torch.arange(len(mask))[mask].long()
        return heads, index

    def prune_linear_layer_shim(layer, index, dim=0):
        """
        Prune a linear layer to keep only entries in index.
        """
        index = index.to(layer.weight.device)
        W = layer.weight.index_select(dim, index).clone().detach()
        if layer.bias is not None:
            if dim == 1:
                b = layer.bias.clone().detach()
            else:
                b = layer.bias.index_select(dim, index).clone().detach()
        new_size = list(layer.weight.size())
        new_size[dim] = len(index)
        new_layer = nn.Linear(new_size[1], new_size[0], bias=layer.bias is not None).to(layer.weight.device)
        new_layer.weight.requires_grad = False
        new_layer.weight.copy_(W.contiguous())
        new_layer.weight.requires_grad = True
        if layer.bias is not None:
            new_layer.bias.requires_grad = False
            new_layer.bias.copy_(b.contiguous())
            new_layer.bias.requires_grad = True
        return new_layer

    if not hasattr(transformers.pytorch_utils, "find_pruneable_heads_and_indices"):
        transformers.pytorch_utils.find_pruneable_heads_and_indices = finding_pruneable_heads_and_indices_shim # type: ignore
        
    if not hasattr(transformers.pytorch_utils, "prune_linear_layer"):
        transformers.pytorch_utils.prune_linear_layer = prune_linear_layer_shim # type: ignore
        
    # Neural-Core: Use stderr for shim logs to avoid JSON-RPC stdout pollution
    sys.stderr.write("Applied Transformers pruning utils shim (In-Line Definition).\n")
except ImportError:
    pass

# -----------------------------------------------------------------------------
# WINDOWS COMPATIBILITY MOCKING for Triton/Flash Attention
# -----------------------------------------------------------------------------
# Windows often lacks triton/flash_attn support which the model might try to import.
# We assign them a mock module to prevent import cascade failure.
from types import ModuleType
import importlib.machinery

# Create mocks with minimal spec to satisfy find_spec checks
triton_mock = ModuleType("triton")
triton_mock.__spec__ = importlib.machinery.ModuleSpec(name="triton", loader=None)
sys.modules["triton"] = triton_mock

flash_mock = ModuleType("flash_attn")
flash_mock.__spec__ = importlib.machinery.ModuleSpec(name="flash_attn", loader=None)
sys.modules["flash_attn"] = flash_mock

logger = logging.getLogger("EXPEDIA.Embedder")

class NucleotideEmbedder:
    """
    @Neural-Core: Handles inference for Nucleotide Transformer v2-50M.
    - Patches config to prevent SwiGLU errors.
    - Forces CPU execution.
    - Standardizes output from 512 -> 768 dimensions.
    """
    def __init__(self):
        # Neural-Core: Air-Gapped Model Loading
        # self.model_name = "InstadeepAI/nucleotide-transformer-v2-50m-multi-species"
        self.model_path = app_config.LOCAL_MODEL_PATH
        self.device = "cpu"
        self.max_length = app_config.EMBEDDING_DIMENSION_MODEL
        
        logger.info(f"Initializing Neural Core from LOCAL ANCHOR: {self.model_path}...")

        try:
            # Load Config & Patch (Local Only)
            config = AutoConfig.from_pretrained(
                self.model_path, 
                local_files_only=True,
                trust_remote_code=True
            )
            # Ensure 4096 patch is still active (even if saved in config, good to enforce)
            config.intermediate_size = app_config.MODEL_INTERMEDIATE_SIZE 
            
            # Neural-Core: Fix AttributeError: 'EsmConfig' object has no attribute 'is_decoder'
            # (Prevents crash when base transformer checks for decoder architecture)
            config.tie_word_embeddings = False
            config.is_decoder = False
            config.add_cross_attention = False

            # Load Tokenizer (Local Only)
            self.tokenizer = AutoTokenizer.from_pretrained(
                self.model_path,
                local_files_only=True,
                trust_remote_code=True
            )
            
            # Load Model (Local Only)
            self.model = AutoModelForMaskedLM.from_pretrained(
                self.model_path, 
                config=config, 
                local_files_only=True,
                trust_remote_code=True
            ).to(self.device)
            
            self.model.eval() # Inference Mode
            logger.info("AIR-GAPPED: Model loaded successfully with localized weights.")
            
        except Exception as e:
            logger.error(f"Failed to load Air-Gapped Model from {self.model_path}: {e}")
            raise

    def generate_embedding(self, sequence: str) -> np.ndarray:
        """
        Generates a 768-dim embedding for a DNA sequence.
        1. Tokenize
        2. Inference (Hidden States)
        3. Mean Pooling
        4. Pad 512 -> 768
        """
        if not sequence or len(sequence) < 10:
            logger.warning("Sequence too short for embedding.")
            return np.zeros(app_config.EMBEDDING_DIMENSION_PADDED, dtype=np.float32)

        inputs = self.tokenizer(
            sequence, 
            return_tensors="pt", 
            padding=True, 
            truncation=True, 
            max_length=1000 # Limit sequence length
        ).to(self.device)

        with torch.no_grad():
            outputs = self.model(**inputs, output_hidden_states=True)
            
            # Extract last hidden state
            # shape: (batch_size, seq_len, hidden_dim=512)
            hidden_states = outputs.hidden_states[-1] 
            
            # Mean Pooling (exclude padding tokens if possible, but simple mean here for speed)
            # attention_mask = inputs['attention_mask']
            # masked_hidden = hidden_states * attention_mask.unsqueeze(-1)
            # mean_embedding = masked_hidden.sum(dim=1) / attention_mask.sum(dim=1, keepdim=True)
            
            mean_embedding = torch.mean(hidden_states, dim=1)
            
            # Detach to numpy
            embedding_512 = mean_embedding.squeeze().numpy()

        # Standardization: Pad 512 -> 768
        embedding_768 = np.zeros(app_config.EMBEDDING_DIMENSION_PADDED, dtype=np.float32)
        embedding_768[:app_config.EMBEDDING_DIMENSION_MODEL] = embedding_512
        
        return embedding_768
