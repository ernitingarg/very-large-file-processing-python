

class Record:
    def __init__(self, record_id: int, record_value: int) -> None:
        """
        Represents a Record with a unique identifier and a numeric value.

        :param record_id: The unique identifier of the record (int).
        :param record_value: The numeric value associated with the record (int).
        """
        self.record_id = record_id
        self.record_value = record_value

    def __lt__(self, other: 'Record') -> bool:
        """
        Comparison method used by heapq to compare records based on their values.

        :param other: Another Record object to compare with.
        :return: True if this record's value is less than the other record's value, False otherwise.
        """
        return self.record_value < other.record_value

    def __eq__(self, other: 'Record') -> bool:
        """
        Comparison method used to check if two records have the same value.

        :param other: Another Record object to compare with.
        :return: True if both records have the same value, False otherwise.
        """
        return self.record_value == other.record_value

    def __str__(self):
        """
        String representation of the Record object.
        """
        return f"Record ID: {self.record_id}, Record Value: {self.record_value}"
