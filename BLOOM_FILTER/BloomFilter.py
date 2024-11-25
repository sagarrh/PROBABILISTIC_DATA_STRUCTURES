import mmh3  # MurmurHash3 for better hash functions
import math
import csv
from typing import List, Optional
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class BloomFilter:
    def __init__(self, expected_elements: int, false_positive_rate: float = 0.01):
        """
        Initialize Bloom Filter with optimal size and number of hash functions.
        
        Args:
            expected_elements: Expected number of elements to be inserted
            false_positive_rate: Desired false positive probability
        """
        self.size = self._calculate_optimal_size(expected_elements, false_positive_rate)
        self.hash_count = self._calculate_optimal_hash_count(expected_elements)
        self.bit_array = [0] * self.size
        self.elements_count = 0
        
        logging.info(f"Initialized Bloom Filter with size: {self.size}, hash functions: {self.hash_count}")

    def _calculate_optimal_size(self, n: int, p: float) -> int:
        """Calculate optimal bit array size."""
        m = -(n * math.log(p)) / (math.log(2) ** 2)
        return math.ceil(m)

    def _calculate_optimal_hash_count(self, n: int) -> int:
        """Calculate optimal number of hash functions."""
        k = (self.size / n) * math.log(2)
        return math.ceil(k)

    def _get_hash_values(self, item: str) -> List[int]:
        """
        Generate hash values for an item using MurmurHash3.
        Uses double hashing technique to generate multiple hash values.
        """
        hash_values = []
        for seed in range(self.hash_count):
           # hash_val = mmh3.hash(str(item), seed) % self.size
            hash_val=mmh3.hash(str(item),seed)%self.size
            hash_values.append(abs(hash_val))
        return hash_values

    def add(self, item: str) -> None:
        """Add an item to the Bloom filter."""
        hash_values = self._get_hash_values(item)
        for hash_val in hash_values:
            self.bit_array[hash_val] = 1
        self.elements_count += 1

    def check(self, item: str) -> str:
        """
        Check if an item might be in the set.
        
        Returns:
            str: "Definitely not present" if item is definitely not in set,
                 "Probably present" if item might be in set
        """
        hash_values = self._get_hash_values(item)
        for hash_val in hash_values:
            if self.bit_array[hash_val] == 0:
                return "Definitely not present"
        return "Probably present"

    def get_current_false_positive_rate(self) -> float:
        """Calculate the current false positive rate based on elements added."""
        if self.elements_count == 0:
            return 0.0
        return (1 - math.exp(-self.hash_count * self.elements_count / self.size)) ** self.hash_count

class BloomFilterCSVHandler:
    def __init__(self, expected_elements: int, false_positive_rate: float = 0.01):
        self.bloom_filter = BloomFilter(expected_elements, false_positive_rate)

    def process_csv(self, file_path: str, column_name: Optional[str] = None) -> None:
        """
        Process CSV file and add items to Bloom filter.
        
        Args:
            file_path: Path to CSV file
            column_name: Name of column to process. If None, processes first column
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
                    self.bloom_filter.add(str(value))
                    processed += 1
                    
                    if processed % 10000 == 0:
                        logging.info(f"Processed {processed} rows")

                logging.info(f"Finished processing {processed} rows")
                logging.info(f"Current false positive rate: {self.bloom_filter.get_current_false_positive_rate():.4f}")

        except Exception as e:
            logging.error(f"Error processing CSV file: {str(e)}")
            raise

def main():
    # Example usage
    try:
        # Initialize with expected elements and desired false positive rate
        handler = BloomFilterCSVHandler(expected_elements=1000000, false_positive_rate=0.01)
        
        # Process CSV file
        file_path = input("File path:")
        column_name = input("Coloumn to scan:")  # Replace with your column name
        handler.process_csv(file_path, column_name)
        
        # Example queries
        while True:
            query = input('Enter a name to check (or "exit" to quit): ')
            if query.lower() == 'exit':
                break
            result = handler.bloom_filter.check(query)
            print(f"Result: {result}")
            

    except Exception as e:
        logging.error(f"Error in main: {str(e)}")

if __name__ == "__main__":

    
    main()