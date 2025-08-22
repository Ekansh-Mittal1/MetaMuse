#!/usr/bin/env python3

import json
import numpy as np
import torch
from transformers import AutoTokenizer, AutoModel
import faiss  # This will be faiss-gpu for GPU operations
import pickle
from pathlib import Path
import warnings
from tqdm import tqdm

warnings.filterwarnings("ignore")


class OntologySemanticSearch:
    """
    Semantic search system for ontology dictionaries using SapBERT and FAISS-GPU.
    Uses GPU for both transformers model inference and FAISS vector search.
    """

    def __init__(
        self,
        dictionary_path,
        model_name="cambridgeltl/SapBERT-from-PubMedBERT-fulltext",
        use_local_cache_only=True,
    ):
        """
        Initialize the semantic search system.

        Args:
            dictionary_path (str): Path to the JSON dictionary file
            model_name (str): Name of the sentence transformer model to use
            use_local_cache_only (bool): If True, only use locally cached models. If False, download from Hugging Face.
        """
        self.dictionary_path = dictionary_path
        self.model_name = model_name
        self.use_local_cache_only = use_local_cache_only
        self.tokenizer = None
        self.model = None
        self.index = None
        self.id_map = {}
        self.terms = []
        self.ids = []
        self.term_vectors = None

        # Check for GPU availability
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # Check for FAISS GPU support
        self.use_gpu_faiss = torch.cuda.is_available() and hasattr(
            faiss, "GpuIndexFlatIP"
        )

        # Load the dictionary
        self._load_dictionary()

        # Initialize the model
        self._initialize_model()

    def _load_dictionary(self):
        """Load the ontology dictionary from JSON file."""
        with open(self.dictionary_path, "r") as f:
            term_to_id = json.load(f)

        # Extract terms and IDs
        self.terms = list(term_to_id.keys())
        self.ids = list(term_to_id.values())

        # Create ID mapping for FAISS index
        self.id_map = {i: (self.terms[i], self.ids[i]) for i in range(len(self.terms))}

    def _initialize_model(self):
        """Initialize the sentence transformer model on GPU."""
        
        # Set up cache directory relative to this file
        cache_dir = Path(__file__).parent / "model_cache"
        cache_dir.mkdir(exist_ok=True)
        
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(
                self.model_name,
                cache_dir=str(cache_dir),
                local_files_only=self.use_local_cache_only
            )
            self.model = AutoModel.from_pretrained(
                self.model_name,
                cache_dir=str(cache_dir),
                local_files_only=self.use_local_cache_only
            )
        except Exception as e:
            if self.use_local_cache_only:
                raise RuntimeError(
                    f"Failed to load model from local cache. Please run the pre-download script first.\n"
                    f"Error: {e}\n"
                    f"Cache directory: {cache_dir}"
                ) from e
            else:
                # Fallback to downloading if local cache fails
                print(f"⚠️  Local cache failed, downloading model: {e}")
                self.tokenizer = AutoTokenizer.from_pretrained(
                    self.model_name,
                    cache_dir=str(cache_dir)
                )
                self.model = AutoModel.from_pretrained(
                    self.model_name,
                    cache_dir=str(cache_dir)
                )

        # Move model to GPU
        self.model = self.model.to(self.device)

        # Set model to evaluation mode
        self.model.eval()
    
    def is_model_cached(self):
        """Check if the model is available in the local cache."""
        cache_dir = Path(__file__).parent / "model_cache"
        model_dir = cache_dir / "models--cambridgeltl--SapBERT-from-PubMedBERT-fulltext"
        return model_dir.exists()
    
    def get_cache_info(self):
        """Get information about the model cache."""
        cache_dir = Path(__file__).parent / "model_cache"
        model_dir = cache_dir / "models--cambridgeltl--SapBERT-from-PubMedBERT-fulltext"
        
        if not model_dir.exists():
            return {
                "cached": False,
                "cache_dir": str(cache_dir),
                "model_dir": str(model_dir),
                "size": "N/A"
            }
        
        # Calculate cache size
        total_size = 0
        file_count = 0
        for file_path in model_dir.rglob("*"):
            if file_path.is_file():
                total_size += file_path.stat().st_size
                file_count += 1
        
        return {
            "cached": True,
            "cache_dir": str(cache_dir),
            "model_dir": str(model_dir),
            "size_mb": round(total_size / (1024 * 1024), 2),
            "file_count": file_count
        }

    def encode(self, text):
        """
        Encode a text string into a dense vector using GPU.

        Args:
            text (str): Text to encode

        Returns:
            torch.Tensor: Encoded vector
        """
        inputs = self.tokenizer(
            text, return_tensors="pt", truncation=True, padding=True, max_length=512
        )

        # Move inputs to GPU
        inputs = {k: v.to(self.device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = self.model(**inputs)

        # Use [CLS] token representation and move to CPU for FAISS
        return outputs.last_hidden_state[:, 0, :].squeeze().cpu()

    def build_index(self):
        """Build FAISS-GPU index for all dictionary terms using GPU encoding."""

        # Encode all terms
        term_vectors = []
        for term in tqdm(self.terms, desc="Encoding terms", unit="terms"):
            vec = self.encode(term)
            term_vectors.append(vec.numpy())

        # Stack vectors
        self.term_vectors = np.stack(term_vectors)

        # Build FAISS index with GPU support
        dim = self.term_vectors.shape[1]

        if self.use_gpu_faiss:
            # Use GPU FAISS for building and searching

            # Create CPU index first
            cpu_index = faiss.IndexFlatIP(dim)

            # Normalize vectors for cosine similarity
            faiss.normalize_L2(self.term_vectors)

            # Add vectors to CPU index
            cpu_index.add(self.term_vectors)

            # Convert to GPU index
            res = faiss.StandardGpuResources()
            self.index = faiss.index_cpu_to_gpu(res, 0, cpu_index)

        else:
            # Fallback to CPU FAISS

            # Use IndexFlatIP for inner product (cosine similarity when normalized)
            self.index = faiss.IndexFlatIP(dim)

            # Normalize vectors for cosine similarity
            faiss.normalize_L2(self.term_vectors)
            self.index.add(self.term_vectors)

            # Set number of threads for CPU operations
            faiss.omp_set_num_threads(4)  # Adjust based on your CPU cores

    def search(self, query, k=5):
        """
        Search for similar terms in the dictionary using GPU.

        Args:
            query (str): Query text
            k (int): Number of top results to return

        Returns:
            list: List of tuples (term, ontology_id, score)
        """

        if self.index is None:
            raise ValueError("Index not built. Call build_index() first.")

        # Encode query using GPU
        query_vec = self.encode(query).unsqueeze(0).numpy()
        faiss.normalize_L2(query_vec)

        # Search using FAISS (GPU or CPU)
        D, I = self.index.search(query_vec, k)

        # Format results
        results = []
        for rank, (idx, score) in enumerate(zip(I[0], D[0])):
            term, ont_id = self.id_map[idx]
            results.append((term, ont_id, float(score)))

        return results

    def save_index(self, output_dir="src/normalization/semantic_indexes"):
        """Save the FAISS index and metadata."""
        import os

        os.makedirs(output_dir, exist_ok=True)

        # Convert GPU index to CPU for saving if needed
        if self.use_gpu_faiss and hasattr(self.index, "index"):
            # If it's a GPU index, convert to CPU for saving
            cpu_index = faiss.index_gpu_to_cpu(self.index)
            index_path = (
                Path(output_dir) / f"{Path(self.dictionary_path).stem}_faiss_gpu.index"
            )
            faiss.write_index(cpu_index, str(index_path))
        else:
            # CPU index can be saved directly
            index_path = (
                Path(output_dir) / f"{Path(self.dictionary_path).stem}_faiss_cpu.index"
            )
            faiss.write_index(self.index, str(index_path))

        # Save metadata
        metadata_path = (
            Path(output_dir) / f"{Path(self.dictionary_path).stem}_metadata.pkl"
        )
        metadata = {
            "id_map": self.id_map,
            "terms": self.terms,
            "ids": self.ids,
            "model_name": self.model_name,
            "faiss_type": "gpu" if self.use_gpu_faiss else "cpu",
            "device_used": str(self.device),
        }

        with open(metadata_path, "wb") as f:
            pickle.dump(metadata, f)

    def load_index(self, output_dir="src/normalization/semantic_indexes"):
        """Load a previously saved FAISS index and metadata."""
        # Try GPU index first, then CPU index
        gpu_index_path = (
            Path(output_dir) / f"{Path(self.dictionary_path).stem}_faiss_gpu.index"
        )
        cpu_index_path = (
            Path(output_dir) / f"{Path(self.dictionary_path).stem}_faiss_cpu.index"
        )
        metadata_path = (
            Path(output_dir) / f"{Path(self.dictionary_path).stem}_metadata.pkl"
        )

        if not metadata_path.exists():
            self.build_index()
            return

        # Load metadata first
        with open(metadata_path, "rb") as f:
            metadata = pickle.load(f)

        # Load index based on availability
        if self.use_gpu_faiss and gpu_index_path.exists():
            cpu_index = faiss.read_index(str(gpu_index_path))
            res = faiss.StandardGpuResources()
            self.index = faiss.index_cpu_to_gpu(res, 0, cpu_index)
        elif cpu_index_path.exists():
            self.index = faiss.read_index(str(cpu_index_path))
        else:
            self.build_index()
            return

        self.id_map = metadata["id_map"]
        self.terms = metadata["terms"]
        self.ids = metadata["ids"]


def main():
    """Main function to demonstrate semantic search for MONDO ontology using GPU encoding and FAISS-GPU."""

    # Initialize semantic search for MONDO
    mondo_search = OntologySemanticSearch(
        "src/normalization/dictionaries/mondo_terms.json"
    )

    # Build or load index
    mondo_search.load_index()

    # Test queries
    test_queries = [
        "diabetes",
        "cancer",
        "heart disease",
        "mental disorder",
        "genetic condition",
        "infectious disease",
        "autoimmune disease",
        "neurological disorder",
    ]

    for query in test_queries:
        results = mondo_search.search(query, k=5)

        for i, (term, ont_id, score) in enumerate(results, 1):
            pass  # Results processed in search method

    # Save the index for future use
    mondo_search.save_index()


if __name__ == "__main__":
    main()
