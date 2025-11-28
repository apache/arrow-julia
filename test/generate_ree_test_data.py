#!/usr/bin/env python3
"""
Generate Arrow IPC files with RunEndEncoded (REE) arrays for testing Arrow.jl
"""

import pyarrow as pa
import pyarrow.ipc as ipc

def create_ree_test_file():
    """Create an Arrow IPC file with various RunEndEncoded array examples"""

    print(f"PyArrow version: {pa.__version__}")

    # All arrays must have same logical length (10 elements each)

    # Example 1: Simple repeated integers - [1,1,1,1,2,2,3,3,3,3]
    values1 = pa.array([1, 2, 3], type=pa.int64())
    run_ends1 = pa.array([4, 6, 10], type=pa.int32())
    ree1 = pa.RunEndEncodedArray.from_arrays(run_ends1, values1)

    # Example 2: Float values with nulls - [1.0,1.0,1.0,1.0,null,null,2.0,2.0,2.0,2.0]
    values2 = pa.array([1.0, None, 2.0], type=pa.float64())
    run_ends2 = pa.array([4, 6, 10], type=pa.int32())
    ree2 = pa.RunEndEncodedArray.from_arrays(run_ends2, values2)

    # Example 3: String values - ["hello","hello","hello","world","world","foo","foo","foo","foo","foo"]
    values3 = pa.array(['hello', 'world', 'foo'], type=pa.string())
    run_ends3 = pa.array([3, 5, 10], type=pa.int32())
    ree3 = pa.RunEndEncodedArray.from_arrays(run_ends3, values3)

    # Example 4: Using Int16 run ends - [100,100,100,100,100,200,200,200,200,200]
    values4 = pa.array([100, 200], type=pa.int32())
    run_ends4 = pa.array([5, 10], type=pa.int16())
    ree4 = pa.RunEndEncodedArray.from_arrays(run_ends4, values4)

    # Example 5: Single run - [42,42,42,42,42,42,42,42,42,42]
    values5 = pa.array([42], type=pa.int64())
    run_ends5 = pa.array([10], type=pa.int32())
    ree5 = pa.RunEndEncodedArray.from_arrays(run_ends5, values5)

    # Example 6: Boolean values - [True,True,True,False,False,True,True,True,True,True]
    values6 = pa.array([True, False, True], type=pa.bool_())
    run_ends6 = pa.array([3, 5, 10], type=pa.int32())
    ree6 = pa.RunEndEncodedArray.from_arrays(run_ends6, values6)

    # Create a table with all examples
    table = pa.table({
        'ree_int': ree1,
        'ree_float_with_nulls': ree2,
        'ree_string': ree3,
        'ree_int16_ends': ree4,
        'ree_single_run': ree5,
        'ree_bool': ree6,
    })

    print("\nTable schema:")
    print(table.schema)
    print(f"\nNumber of rows: {len(table)}")

    # Print decoded values for verification
    print("\nDecoded values:")
    for col_name in table.column_names:
        col = table[col_name]
        decoded = col.to_pylist()
        print(f"{col_name}: {decoded}")

    # Write to Arrow IPC file
    output_path = 'test_ree_data.arrow'
    with ipc.RecordBatchFileWriter(output_path, table.schema) as writer:
        writer.write_table(table)

    print(f"\n✓ Successfully wrote REE test data to: {output_path}")

    # Verify we can read it back
    with ipc.open_file(output_path) as reader:
        table_read = reader.read_all()
        print(f"✓ Verified: read back {len(table_read)} rows")

    return output_path

def create_simple_ree_file():
    """Create a minimal REE file for initial testing"""

    # Very simple case: [1, 1, 1, 2, 2]
    values = pa.array([1, 2], type=pa.int64())
    run_ends = pa.array([3, 5], type=pa.int32())
    ree = pa.RunEndEncodedArray.from_arrays(run_ends, values)

    table = pa.table({'simple_ree': ree})

    output_path = 'test_ree_simple.arrow'
    with ipc.RecordBatchFileWriter(output_path, table.schema) as writer:
        writer.write_table(table)

    print(f"✓ Created simple REE file: {output_path}")
    print(f"  Decoded values: {table['simple_ree'].to_pylist()}")

    return output_path

if __name__ == '__main__':
    print("Generating RunEndEncoded Arrow test files...\n")

    try:
        # Create both comprehensive and simple test files
        create_simple_ree_file()
        print()
        create_ree_test_file()

        print("\n" + "="*60)
        print("Test data generation complete!")
        print("="*60)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("\nNote: RunEndEncoded support was added in PyArrow 13.0.0")
        print("Please upgrade: pip install pyarrow>=13.0.0")
        raise
