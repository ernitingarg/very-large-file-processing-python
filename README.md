# File Data Processor

This solution is designed to find the unique IDs associated with the X-largest values in the rightmost column of a file with the specified format. The program reads the input data from either a file or standard input (stdin) and processes it to produce the desired output. The program also includes error handling to handle various scenarios.

## Input data format

The input data should be in the following fixed format:

```
<unique record identifier><white_space><numeric value>
```

For example:

```
1426828011 9
1426828028 350
1426828037 25
1426828056 231
1426828058 109
1426828066 111
```

## Output format

The program prints a list of the unique IDs associated with the X-largest values in the rightmost column, where X is specified as an input parameter. For given X=3, the above input should produce below output.
Note: The order of the output list may vary and does not follow any particular order.

```
1426828028
1426828066
1426828056
```

## Algorithm

The solution uses a min-heap data structure to efficiently find the X-largest values in the rightmost column while processing the input data. The `Record` class implements the comparison methods required by the min-heap to compare records based on their values. The `RecordUtils` class provides static method(s) to find the unique IDs associated with the X-largest values.

- Initialize an empty min-heap to store the X-largest records.
- Process each line of input data.
- For each line, extract the unique record identifier and numeric value.
- Create a new `Record` object with the unique identifier and numeric value.
- If the min-heap has not reached its capacity (X), push the current record to the heap.
- If the min-heap is full, push the current recird into the heap and simultaneously pop the smallest element (root) of the heap.
- Repeat above steps until all input data is processed.
- Extract the unique IDs from the X-largest records in the min-heap and return them as the result.

## Core and Thread Parallelism

The solution leverages both core and thread parallelism to optimize the processing of input data.

- The number of CPU cores available on the system is determined using multiprocessing.cpu_count().
- The input data is split into smaller chunks to distribute the workload among threads.
- Each chunk of data is processed concurrently by separate threads, which significantly reduces the processing time for large input datasets.
- The results from all chunks are then merged into a single list. The merged list contains all the records from different chunks.
- Finally, the X-largest values are extracted from the merged list

## Time Complexity

For a given min-heap of size X, lets assume the total number of records in the input data is N.

- Reading and parsing each line of input data: O(N)
- Heap insertion and extraction (should be equal to height of the heap): O(log X) for each record
- Overall time complexity: `O(N log X)`

## Space Complexity

For a given min-heap of size X,

- Min-heap to store X-largest records: O(X)
- Additional variables for processing: O(1)
- Overall space complexity: `O(X)`

## Usage

- Open the Command Prompt (CMD) or PowerShell.
- Navigate to this current directoty
- To read the input data from the standard input (stdin)

```
python main.py
```

- To read the data from a file

```
python main.py data.txt
```

- After running the script, the program will prompt you to enter the value of X (the number of largest values to find). Enter a positive integer value for X.

## Unit tests

Please run below command to execute unit tests

```
python -m unittest test_record_utils.py
```
