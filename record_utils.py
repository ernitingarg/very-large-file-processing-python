from concurrent.futures import ThreadPoolExecutor
import heapq
import multiprocessing
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

        # Number of CPU cores available on the system
        num_cores = multiprocessing.cpu_count()

        # Calculate chunk size to distribute data more evenly among threads
        chunk_size = (len(data_lines) + num_cores - 1) // num_cores
        print(
            f"Data will be split into {chunk_size} chunks and "
            f"will be processed in parallel by a maximum of {num_cores} threads."
        )

        # Split data_lines into smaller chunks
        chunks = [data_lines[i:i + chunk_size]
                  for i in range(0, len(data_lines), chunk_size)]

        # Process chunks in parallel using ThreadPoolExecutor with dynamic max workers
        with ThreadPoolExecutor(max_workers=num_cores) as executor:
            results = list(executor.map(RecordUtils.find_largest_records_in_chunk, chunks, [
                           top_x] * num_cores))

        # Merge the results from all chunks into a single list
        merged_results = [
            record for chunk_result in results for record in chunk_result]

        # Get the X-largest values from the merged results
        x_largest = heapq.nlargest(
            top_x, merged_results, key=lambda x: x.record_value)

        # Extract the IDs from the X-largest records
        return [record.record_id for record in x_largest]
