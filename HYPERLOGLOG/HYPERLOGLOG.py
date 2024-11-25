import math
import mmh3
from typing import List, Optional, Dict
import numpy as np
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
import csv
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@dataclass
class EstimatorParams:
    """Parameters for cardinality estimators."""
    precision: int  # Number of bits for register index
    num_registers: int  # 2^precision
    
    @classmethod
    def from_precision(cls, precision: int) -> 'EstimatorParams':
        """Create parameters from precision bits."""
        if not (4 <= precision <= 16):
            raise ValueError("Precision must be between 4 and 16")
        return cls(
            precision=precision,
            num_registers=1 << precision
        )

class BaseLogEstimator(ABC):
    """Base class for LogLog-based cardinality estimators."""
    
    def __init__(self, precision: int):
        """
        Initialize estimator.
        
        Args:
            precision: Number of bits for register indexing
        """
        self.params = EstimatorParams.from_precision(precision)
        self.registers = np.zeros(self.params.num_registers, dtype=np.int8)
        self.alpha = self._get_alpha(self.params.num_registers)
        
        logging.info(f"Initialized {self.__class__.__name__} with {self.params.num_registers} registers")
    
    @staticmethod
    def _get_alpha(m: int) -> float:
        """Calculate alpha constant based on number of registers."""
        if m == 16:
            return 0.673
        elif m == 32:
            return 0.697
        elif m == 64:
            return 0.709
        else:
            return 0.7213 / (1 + 1.079 / m)
    
    def _hash(self, item: str) -> int:
        """Generate 32-bit hash for item."""
        return mmh3.hash(str(item), signed=False)
    
    def _get_register_index(self, hash_value: int) -> int:
        """Extract register index from hash value."""
        return hash_value & (self.params.num_registers - 1)
    
    def _get_leading_zeros(self, hash_value: int) -> int:
        """Count leading zeros in remaining hash bits."""
        value = hash_value >> self.params.precision
        return min(32 - self.params.precision, (value | 1).bit_length() - 1)
    
    @abstractmethod
    def add(self, item: str) -> None:
        """Add item to estimator."""
        pass
    
    @abstractmethod
    def estimate(self) -> int:
        """Estimate cardinality."""
        pass

class LogLog(BaseLogEstimator):
    """LogLog cardinality estimator."""
    
    def add(self, item: str) -> None:
        """
        Add item to LogLog estimator.
        
        Args:
            item: Item to add
        """
        hash_val = self._hash(item)
        idx = self._get_register_index(hash_val)
        zeros = self._get_leading_zeros(hash_val)
        self.registers[idx] = max(self.registers[idx], zeros)
    
    def estimate(self) -> int:
        """
        Estimate cardinality using LogLog algorithm.
        
        Returns:
            Estimated cardinality
        """
        harmonic_mean = 1 / np.mean(2.0 ** -self.registers)
        return int(self.alpha * self.params.num_registers * harmonic_mean)

class SuperLogLog(BaseLogEstimator):
    """SuperLogLog cardinality estimator with truncation."""
    
    def __init__(self, precision: int, truncate_percentage: float = 0.7):
        """
        Initialize SuperLogLog estimator.
        
        Args:
            precision: Number of bits for register indexing
            truncate_percentage: Percentage of largest values to truncate
        """
        super().__init__(precision)
        self.truncate_threshold = int(self.params.num_registers * truncate_percentage)
    
    def add(self, item: str) -> None:
        """
        Add item to SuperLogLog estimator.
        
        Args:
            item: Item to add
        """
        hash_val = self._hash(item)
        idx = self._get_register_index(hash_val)
        zeros = self._get_leading_zeros(hash_val)
        self.registers[idx] = max(self.registers[idx], zeros)
    
    def estimate(self) -> int:
        """
        Estimate cardinality using SuperLogLog algorithm.
        
        Returns:
            Estimated cardinality
        """
        sorted_registers = np.sort(self.registers)
        truncated_registers = sorted_registers[:self.truncate_threshold]
        harmonic_mean = 1 / np.mean(2.0 ** -truncated_registers)
        return int(self.alpha * self.params.num_registers * harmonic_mean)

class HyperLogLog(BaseLogEstimator):
    """HyperLogLog cardinality estimator."""
    
    def add(self, item: str) -> None:
        """
        Add item to HyperLogLog estimator.
        
        Args:
            item: Item to add
        """
        hash_val = self._hash(item)
        idx = self._get_register_index(hash_val)
        zeros = self._get_leading_zeros(hash_val)
        self.registers[idx] = max(self.registers[idx], zeros)
    
    def estimate(self) -> int:
        """
        Estimate cardinality using HyperLogLog algorithm.
        
        Returns:
            Estimated cardinality
        """
        harmonic_mean = 1 / np.mean(2.0 ** -self.registers)
        estimate = self.alpha * self.params.num_registers * harmonic_mean
        
        # Apply corrections for small and large ranges
        if estimate <= 2.5 * self.params.num_registers:  # Small range correction
            num_zeros = np.sum(self.registers == 0)
            if num_zeros > 0:
                estimate = self.params.num_registers * math.log(self.params.num_registers / num_zeros)
        elif estimate > 2**32 / 30:  # Large range correction
            estimate = -2**32 * math.log(1 - estimate / 2**32)
            
        return int(estimate)

class CardinalityProcessor:
    """Handler for processing data streams with cardinality estimators."""
    
    def __init__(self, precision: int = 10):
        """
        Initialize processor with all estimators.
        
        Args:
            precision: Precision bits for estimators
        """
        self.loglog = LogLog(precision)
        self.superloglog = SuperLogLog(precision)
        self.hyperloglog = HyperLogLog(precision)
    
    def process_item(self, item: str) -> None:
        """
        Process a single item through all estimators.
        
        Args:
            item: Item to process
        """
        self.loglog.add(item)
        self.superloglog.add(item)
        self.hyperloglog.add(item)
    
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
                    self.process_item(str(value))
                    
                    processed += 1
                    if processed % batch_size == 0:
                        self._log_estimates(processed)
                
                self._log_estimates(processed)
        
        except Exception as e:
            logging.error(f"Error processing CSV file: {str(e)}")
            raise
    
    def _log_estimates(self, processed: int) -> None:
        """Log current cardinality estimates."""
        logging.info(f"Processed {processed} items")
        logging.info(f"LogLog estimate: {self.loglog.estimate():,}")
        logging.info(f"SuperLogLog estimate: {self.superloglog.estimate():,}")
        logging.info(f"HyperLogLog estimate: {self.hyperloglog.estimate():,}")

def main():
    """Example usage of cardinality estimators."""
    try:
        processor = CardinalityProcessor(precision=10)
        
        while True:
            print("\nCardinality Estimator Operations:")
            print("1. Add item")
            print("2. Process CSV file")
            print("3. Show estimates")
            print("4. Exit")
            
            choice = input("Enter your choice (1-4): ")
            
            if choice == '1':
                item = input("Enter item to add: ")
                processor.process_item(item)
                print("\nCurrent estimates:")
                processor._log_estimates(1)
            
            elif choice == '2':
                file_path = input("Enter CSV file path: ")
                column_name = input("Enter column name (or press Enter for first column): ") or None
                processor.process_csv(file_path, column_name)
            
            elif choice == '3':
                print("\nCurrent estimates:")
                processor._log_estimates(0)
            
            elif choice == '4':
                print("Exiting...")
                break
            
            else:
                print("Invalid choice. Please try again.")
    
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main()