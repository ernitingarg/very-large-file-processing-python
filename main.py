import os
from typing import Generator, List
import sys
from record_utils import RecordUtils


def read_input_data(file_path=None) -> Generator[str, None, None]:
    """
    Read input data from a file or stdin.

    Args:
        file_path (str): The path to the input file or None if reading from stdin.

    Returns:
        list: A generator yielding data lines.
    """
    if file_path:
        try:
            with open(file_path, 'r') as file:
                for line in file:
                    yield line
        except Exception as e:
            raise Exception(
                f"Error occurred while reading file '{file_path}': {e}") from e
    else:
        print("Enter data in the format '<unique record identifier> <numeric value>', one record per line:")
        while True:
            line = input()
            if not line:
                break
            yield line


def process_input_data(file_path=None):
    """
    Process file content and find the unique IDs associated with the X-largest values in the rightmost column.

    Args:
        file_path (str): The path to the input file or None if reading from stdin.
    """
    try:
        if file_path and not os.path.exists(file_path):
            print(f"File '{file_path}' not found.")
            return

        top_x = int(input("Enter the value of X: "))
        if top_x <= 0:
            print("Error: X must be a non-zero positive integer.")
            return

        data_lines = read_input_data(file_path)
        top_x_ids = RecordUtils.find_largest_ids_parallel(data_lines, top_x)

        print("Unique IDs of the X-largest values in the rightmost column:")
        for record_id in top_x_ids:
            print(record_id)

    except ValueError:
        print("Error: Invalid value of X, please enter a non-zero positive integer.")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    if len(sys.argv) == 2:
        file_path = sys.argv[1]
    else:
        file_path = None

    try:
        process_input_data(file_path)
    except KeyboardInterrupt:
        print("\nAborted by the user.")
        sys.exit(0)
