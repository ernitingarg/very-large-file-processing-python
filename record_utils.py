from concurrent.futures import ProcessPoolExecutor, as_completed
import heapq
from record import Record
from typing import List, Iterable


class RecordUtils:
    """
    Utility class to work with Record objects.
    """

    @staticmethod
    def find_largest_records_in_chunk(chunk: Iterable[str], top_x: int) -> List[Record]:
        """
        Find the X-largest records in a chunk.

        Args:
            chunk (Iterable[str]): An iterable providing lines of input data in 
                                   the format "<unique record identifier> <numeric value>"
            top_x (int): The number of largest values to find.

        Returns:
            List[Record]: A list of X-largest records in the chunk
        """

        # Min-heap to get the X-largest values in the rightmost column for a given chunk
        min_heap = []

        for line in chunk:
            items = line.strip().split()
            if len(items) != 2:
                print(f"Warning: Invalid input line - {line}")
                continue

            try:
                record_id, record_value = map(int, items)
                current_record = Record(record_id, record_value)

                if len(min_heap) < top_x:
                    # If we haven't collected enough records yet,
                    # add the current one to the min-heap.
                    heapq.heappush(min_heap, current_record)
                else:
                    # If the min heap is already full,
                    # add the current record and remove the smallest element (the root).
                    heapq.heappushpop(min_heap, current_record)
            except ValueError:
                # When input line contains invalid values.
                print(f"Warning: Invalid input line - {line}")

        return min_heap

    @staticmethod
    def find_largest_ids_parallel(data_lines: Iterable[str], top_x: int) -> List[int]:
        """
        Find the unique IDs associated with the X-largest values in parallel.

        Args:
            data_lines (Iterable[str]): An iterable providing lines of input data in 
                                        the format "<unique record identifier> <numeric value>"
            top_x (int): The number of largest values to find.

        Returns:
            List[int]: A list of unique IDs associated with the X-largest values.
        """

        if not data_lines:
            return []

        # Calculate chunk size to distribute data more evenly among threads
        chunk_size = 10000

        merged_results = []
        # Process chunks in parallel using ProcessPoolExecutor with dynamic max workers
        with ProcessPoolExecutor() as executor:
            futures = []
            chunk = []
            for line in data_lines:
                chunk.append(line)
                if len(chunk) == chunk_size:
                    future = executor.submit(
                        RecordUtils.find_largest_records_in_chunk,
                        chunk,
                        top_x)
                    futures.append(future)
                    chunk = []

            # remaining chunk less than chunk size
            if chunk:
                future = executor.submit(
                    RecordUtils.find_largest_records_in_chunk,
                    chunk,
                    top_x)
                futures.append(future)

            for chunk_result in as_completed(futures):
                merged_results.extend(chunk_result.result())

        # Get the X-largest values from the merged results
        x_largest = heapq.nlargest(
            top_x, merged_results, key=lambda x: x.record_value)

        # Extract the IDs from the X-largest records
        return [record.record_id for record in x_largest]
