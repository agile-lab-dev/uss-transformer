import unittest

from config import *
from tools import setup, transformation, test_bridge

SETUP = True


class UssTransformation(unittest.TestCase):
    def test_loops(self):
        """
        This test is passed if the algorithm works well in presence of loops, by checking whether the data into the
        bridge are equal to the expected data
        """

        schema = 'loops'
        notes_file = 'notes/loops.sql'

        columns = ['stage', '_key_phonecalls', '_key_emails', '_key_deals', '_key_employees', '_key_customers']
        rows = [
            ['phonecalls', 1, None, None, 'Emp1', 'A'],
            ['phonecalls', 2, None, None, 'Emp1', 'A'],
            ['phonecalls', 3, None, None, 'Emp2', 'B'],
            ['phonecalls', 4, None, None, 'Emp2', 'B'],
            ['phonecalls', 5, None, None, 'Emp2', 'C'],
            ['emails', None, 1, None, 'Emp1', 'C'],
            ['emails', None, 2, None, 'Emp1', 'D'],
            ['emails', None, 3, None, 'Emp1', 'E'],
            ['emails', None, 4, None, 'Emp3', 'A'],
            ['deals', None, None, 1, 'Emp4', 'A'],
            ['deals', None, None, 2, 'Emp4', 'F'],
            ['deals', None, None, 3, 'Emp4', 'G'],
            ['employees', None, None, None, 'Emp1', None],
            ['employees', None, None, None, 'Emp2', None],
            ['employees', None, None, None, 'Emp3', None],
            ['employees', None, None, None, 'Emp4', None],
            ['employees', None, None, None, 'Emp5', None],
            ['employees', None, None, None, 'Emp6', None],
            ['customers', None, None, None, None, 'A'],
            ['customers', None, None, None, None, 'B'],
            ['customers', None, None, None, None, 'C'],
            ['customers', None, None, None, None, 'D'],
            ['customers', None, None, None, None, 'E'],
            ['customers', None, None, None, None, 'F'],
            ['customers', None, None, None, None, 'G'],
            ['customers', None, None, None, None, 'H'],
        ]

        if SETUP:
            setup(schema, DBNAME, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, MINIO_HOST, MINIO_PORT, MINIO_USER,
                  MINIO_PASSWORD, TRINO_HOST, TRINO_PORT, TRINO_USER)

        transformation(schema, TRINO_HOST, TRINO_PORT, TRINO_USER, notes_file)

        is_equal = test_bridge(schema, TRINO_HOST, TRINO_PORT, TRINO_USER, columns, rows)

        self.assertEqual(True, is_equal)


if __name__ == '__main__':
    unittest.main()
