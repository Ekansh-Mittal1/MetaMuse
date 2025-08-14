#!/usr/bin/env python3
"""
CSV to Parquet Converter

This script converts CSV files to parquet format using pandas.
It handles various CSV formats and provides options for data type optimization.
"""

import argparse
import pandas as pd
import os
import sys
from pathlib import Path


def convert_csv_to_parquet(input_file, output_file, separator=',', index_col=None, 
                          optimize_dtypes=True, compression='snappy'):
    """
    Convert a CSV file to parquet format.
    
    Parameters:
    -----------
    input_file : str
        Path to the input CSV file
    output_file : str
        Path to the output parquet file
    separator : str, default=','
        CSV separator character
    index_col : int or str, default=None
        Column to use as index
    optimize_dtypes : bool, default=True
        Whether to optimize data types for memory efficiency
    compression : str, default='snappy'
        Compression method for parquet file
    """
    
    try:
        print(f"Reading CSV file: {input_file}")
        
        # Read the CSV file
        df = pd.read_csv(input_file, sep=separator, index_col=index_col)
        
        print(f"CSV loaded successfully:")
        print(f"  - Shape: {df.shape}")
        print(f"  - Columns: {len(df.columns)}")
        print(f"  - Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        # Optimize data types if requested
        if optimize_dtypes:
            print("Optimizing data types...")
            original_memory = df.memory_usage(deep=True).sum()
            
            # Convert object columns to appropriate types
            for col in df.select_dtypes(include=['object']).columns:
                # Try to convert to numeric if possible
                try:
                    pd.to_numeric(df[col], errors='raise')
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    print(f"  - Converted column '{col}' to numeric")
                except (ValueError, TypeError):
                    # Try to convert to datetime if it looks like dates
                    try:
                        pd.to_datetime(df[col], errors='raise')
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                        print(f"  - Converted column '{col}' to datetime")
                    except (ValueError, TypeError):
                        # Keep as object/string
                        pass
            
            # Convert integer columns to smaller types if possible
            for col in df.select_dtypes(include=['int64']).columns:
                col_min = df[col].min()
                col_max = df[col].max()
                
                if col_min >= -128 and col_max <= 127:
                    df[col] = df[col].astype('int8')
                    print(f"  - Converted column '{col}' to int8")
                elif col_min >= -32768 and col_max <= 32767:
                    df[col] = df[col].astype('int16')
                    print(f"  - Converted column '{col}' to int16")
                elif col_min >= -2147483648 and col_max <= 2147483647:
                    df[col] = df[col].astype('int32')
                    print(f"  - Converted column '{col}' to int32")
            
            # Convert float columns to smaller types if possible
            for col in df.select_dtypes(include=['float64']).columns:
                if df[col].dtype == 'float64':
                    df[col] = pd.to_numeric(df[col], downcast='float')
                    print(f"  - Converted column '{col}' to {df[col].dtype}")
            
            optimized_memory = df.memory_usage(deep=True).sum()
            memory_saved = original_memory - optimized_memory
            print(f"  - Memory saved: {memory_saved / 1024**2:.2f} MB")
        
        # Create output directory if it doesn't exist
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save as parquet
        print(f"Saving to parquet file: {output_file}")
        df.to_parquet(output_file, compression=compression, index=(index_col is not None))
        
        # Verify the file was created
        if os.path.exists(output_file):
            file_size = os.path.getsize(output_file) / 1024**2
            print(f"✅ Conversion completed successfully!")
            print(f"  - Output file: {output_file}")
            print(f"  - File size: {file_size:.2f} MB")
            print(f"  - Compression: {compression}")
        else:
            print("❌ Error: Output file was not created")
            return False
            
    except FileNotFoundError:
        print(f"❌ Error: Input file '{input_file}' not found")
        return False
    except pd.errors.EmptyDataError:
        print(f"❌ Error: Input file '{input_file}' is empty")
        return False
    except pd.errors.ParserError as e:
        print(f"❌ Error parsing CSV file: {e}")
        print("Try specifying a different separator with --separator")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description='Convert CSV files to parquet format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic conversion
  python csv_to_parquet.py input.csv output.parquet
  
  # With custom separator (tab-delimited)
  python csv_to_parquet.py input.tsv output.parquet --separator "\\t"
  
  # With index column
  python csv_to_parquet.py input.csv output.parquet --index-col 0
  
  # Without data type optimization
  python csv_to_parquet.py input.csv output.parquet --no-optimize
  
  # With different compression
  python csv_to_parquet.py input.csv output.parquet --compression gzip
        """
    )
    
    parser.add_argument('input_file', help='Input CSV file path')
    parser.add_argument('output_file', help='Output parquet file path')
    parser.add_argument('--separator', '-s', default=',', 
                       help='CSV separator (default: comma)')
    parser.add_argument('--index-col', '-i', type=int, default=None,
                       help='Column to use as index (0-based)')
    parser.add_argument('--no-optimize', action='store_true',
                       help='Disable data type optimization')
    parser.add_argument('--compression', '-c', default='snappy',
                       choices=['snappy', 'gzip', 'brotli', 'lz4', 'zstd'],
                       help='Compression method (default: snappy)')
    
    args = parser.parse_args()
    
    # Validate input file
    if not os.path.exists(args.input_file):
        print(f"❌ Error: Input file '{args.input_file}' does not exist")
        sys.exit(1)
    
    # Validate file extension
    if not args.input_file.lower().endswith(('.csv', '.tsv', '.txt')):
        print("⚠️  Warning: Input file doesn't have typical CSV extension")
    
    # Convert the file
    success = convert_csv_to_parquet(
        input_file=args.input_file,
        output_file=args.output_file,
        separator=args.separator,
        index_col=args.index_col,
        optimize_dtypes=not args.no_optimize,
        compression=args.compression
    )
    
    if success:
        print("\n🎉 CSV to Parquet conversion completed successfully!")
        sys.exit(0)
    else:
        print("\n💥 Conversion failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()


