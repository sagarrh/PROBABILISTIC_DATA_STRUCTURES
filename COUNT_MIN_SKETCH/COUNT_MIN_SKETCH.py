import mmh3  # MurmurHash3 for better hash functions
import math
import logging
import numpy as np
from typing import List, Optional, Tuple
from dataclasses import dataclass
import sys
import csv
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@dataclass
class CMSParameters:
    """Parameters for Count-Min Sketch initialization."""
    width: int
    depth: int
    epsilon: float  # Error bound
    delta: float    # Probability of error
    
    @classmethod
    def from_error_rate(cls, epsilon: float, delta: float) -> 'CMSParameters':
        """
        Calculate optimal width and depth from error bounds.
        
        Args:
            epsilon: Desired error rate (between 0 and 1)
            delta: Desired probability of error (between 0 and 1)
            
        Returns:
            CMSParameters object with calculated width and depth
        """
        if not (0 < epsilon < 1 and 0 < delta < 1):
            raise ValueError("Epsilon and delta must be between 0 and 1")
            
        width = math.ceil(math.e / epsilon)
        depth = math.ceil(math.log(1 / delta))
        return cls(width=width, depth=depth, epsilon=epsilon, delta=delta)

class CountMinSketch:
    """
    Count-Min Sketch implementation for approximate frequency counting.
    """
    
    def __init__(self, params: CMSParameters):
        """
        Initialize Count-Min Sketch with given parameters.
        
        Args:
            params: CMSParameters object containing width, depth, and error bounds
        """
        self.params = params
        self.width = params.width
        self.depth = params.depth
        self.sketch = np.zeros((self.depth, self.width), dtype=np.int64)
        self.total_items = 0
        
        logging.info(f"Initialized Count-Min Sketch with width={self.width}, depth={self.depth}")
        logging.info(f"Expected error rate: {self.params.epsilon:.4f}")
        logging.info(f"Error probability: {self.params.delta:.4f}")

    def _get_hash_indices(self, item: str) -> List[int]:
        """
        Generate hash indices for an item using MurmurHash3.
        
        Args:
            item: Item to hash
            
        Returns:
            List of hash indices
        """
        indices = []
        for seed in range(self.depth):
            hash_val = mmh3.hash(str(item), seed) % self.width
            indices.append(abs(hash_val))
        return indices

    def add(self, item: str, count: int = 1) -> None:
        """
        Add an item to the sketch with a given count.
        
        Args:
            item: Item to add
            count: Count to add (default: 1)
        """
        if count <= 0:
            raise ValueError("Count must be positive")
            
        indices = self._get_hash_indices(item)
        for i, index in enumerate(indices):
            self.sketch[i][index] += count
        self.total_items += count

    def get_count(self, item: str) -> Tuple[int, float]:
        """
        Get the estimated count of an item.
        
        Args:
            item: Item to query
            
        Returns:
            Tuple of (estimated_count, error_bound)
        """
        indices = self._get_hash_indices(item)
        estimated_count = min(self.sketch[i][index] for i, index in enumerate(indices))
        error_bound = self.total_items * self.params.epsilon
        
        return estimated_count, error_bound

class CMSDataProcessor:
    """Handler for processing data streams with Count-Min Sketch."""
    
    def __init__(self, epsilon: float = 0.01, delta: float = 0.01):
        """
        Initialize processor with error bounds.
        
        Args:
            epsilon: Desired error rate
            delta: Desired probability of error
        """
        params = CMSParameters.from_error_rate(epsilon, delta)
        self.cms = CountMinSketch(params)

    def process_csv(self, file_path: str, column_name: Optional[str] = None, 
                   batch_size: int = 10000) -> None:
        """
        Process items from a CSV file.
        
        Args:
            file_path: Path to CSV file
            column_name: Name of column to process
            batch_size: Number of items to process in each batch
        """
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(f"CSV file not found: {file_path}")

            with open(file_path, 'r') as file:
                reader = csv.DictReader(file) if column_name else csv.reader(file)
                
                if column_name and column_name not in reader.fieldnames:
                    raise ValueError(f"Column '{column_name}' not found in CSV file")

                processed = 0
                for row in reader:
                    value = row[column_name] if column_name else row[0]
                    self.cms.add(str(value))
                    
                    processed += 1
                    if processed % batch_size == 0:
                        logging.info(f"Processed {processed} items")

                logging.info(f"Finished processing {processed} items")

        except Exception as e:
            logging.error(f"Error processing CSV file: {str(e)}")
            raise

def main():
    """Example usage of Count-Min Sketch."""
    try:
        # Initialize processor with error bounds
        processor = CMSDataProcessor(epsilon=0.01, delta=0.01)
        
        # Interactive mode
        while True:
            print("\nCount-Min Sketch Operations:")
            print("1. Add item")
            print("2. Get count")
            print("3. Process CSV file")
            print("4. Exit")
            
            choice = input("Enter your choice (1-4): ")
            
            if choice == '1':
                item = input("Enter item to add: ")
                count = int(input("Enter count (default 1): ") or "1")
                processor.cms.add(item, count)
                print(f"Added {item} with count {count}")
                
            elif choice == '2':
                item = input("Enter item to query: ")
                count, error_bound = processor.cms.get_count(item)
                print(f"Estimated count: {count} ± {error_bound:.2f}")
                
            elif choice == '3':
                file_path = input("Enter CSV file path: ")
                column_name = input("Enter column name (or press Enter for first column): ") or None
                processor.process_csv(file_path, column_name)
                
            elif choice == '4':
                print("Exiting...")
                break
                
            else:
                print("Invalid choice. Please try again.")

    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()