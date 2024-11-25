import random
from typing import List, Set, Callable

class MinHash:
    def __init__(self, num_hashes: int) -> None:
        """
        Initializes the MinHash class with the desired number of hash functions.
        
        Args:
            num_hashes (int): Number of hash functions to use.
        """
        self.num_hashes = num_hashes
        self.hash_functions = self._generate_hash_functions()

    def _generate_hash_functions(self) -> List[Callable[[str], int]]:
        """
        Generates a list of hash functions. Each hash function is simulated
        using a random linear transformation: (a * x + b) % p.

        Returns:
            List[Callable[[str], int]]: List of hash functions.
        """
        max_value = 2**31 - 1  # Large prime number for modulo
        hash_functions = []
        for _ in range(self.num_hashes):
            a = random.randint(1, max_value)
            b = random.randint(0, max_value)
            hash_functions.append(lambda x, a=a, b=b, p=max_value: (a * hash(x) + b) % p)
        return hash_functions

    def compute_signature(self, input_set: Set[str]) -> List[int]:
        """
        Computes the MinHash signature for the given set.
        
        Args:
            input_set (Set[str]): The set for which to compute the signature.
        
        Returns:
            List[int]: MinHash signature as a list of minimum hash values.
        """
        signature = []
        for hash_func in self.hash_functions:
            min_hash_value = min(hash_func(item) for item in input_set)
            signature.append(min_hash_value)
        return signature

    @staticmethod
    def compute_jaccard_similarity(set_a: Set[str], set_b: Set[str]) -> float:
        """
        Computes the exact Jaccard similarity between two sets.
        
        Args:
            set_a (Set[str]): First set.
            set_b (Set[str]): Second set.
        
        Returns:
            float: Jaccard similarity between the two sets.
        """
        intersection = len(set_a.intersection(set_b))
        union = len(set_a.union(set_b))
        return intersection / union if union != 0 else 0.0

    @staticmethod
    def estimate_similarity(signature_a: List[int], signature_b: List[int]) -> float:
        """
        Estimates the similarity between two sets based on their MinHash signatures.
        
        Args:
            signature_a (List[int]): MinHash signature of the first set.
            signature_b (List[int]): MinHash signature of the second set.
        
        Returns:
            float: Estimated Jaccard similarity.
        """
        matches = sum(1 for a, b in zip(signature_a, signature_b) if a == b)
        return matches / len(signature_a)


if __name__ == "__main__":
    set_a = {"sagar", "bharat", "harsora"}
    set_b = {"cat", "fish", "bird","sagar", "harsora"}

    
    num_hashes = 100
    minhash = MinHash(num_hashes)

    signature_a = minhash.compute_signature(set_a)
    signature_b = minhash.compute_signature(set_b)

    true_similarity = MinHash.compute_jaccard_similarity(set_a, set_b)
    print(f"Exact Jaccard Similarity: {true_similarity:.2f}")

    estimated_similarity = MinHash.estimate_similarity(signature_a, signature_b)
    print(f"Estimated Similarity (MinHash): {estimated_similarity:.2f}")
