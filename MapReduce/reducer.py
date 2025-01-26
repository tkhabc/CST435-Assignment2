#!/usr/bin/env python3
# reducer.py with performance tracking

import sys
import time
import resource
import logging
from datetime import datetime

def get_memory_usage():
    """Get current memory usage in megabytes"""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024

def reducer():
    """
    Calculate average temperature per month with performance tracking
    """
    start_time = time.time()
    start_memory = get_memory_usage()
    
    current_month = None
    temperature_sum = 0
    temperature_count = 0
    lines_processed = 0
    error_lines = 0
    months_processed = set()
    
    try:
        for line in sys.stdin:
            try:
                line = line.strip()
                if not line:  # Skip empty lines
                    continue
                    
                month, temperature = line.split('\t')
                temperature = float(temperature)
                
                if current_month == month:
                    temperature_sum += temperature
                    temperature_count += 1
                else:
                    if current_month:
                        average = temperature_sum / temperature_count
                        print(f'{current_month}\t{average:.2f}')
                        months_processed.add(current_month)
                    current_month = month
                    temperature_sum = temperature
                    temperature_count = 1
                
                lines_processed += 1
            except Exception as e:
                sys.stderr.write(f'Error processing line: {line}, Error: {str(e)}\n')
                error_lines += 1
                continue
        
        # Process final month
        if current_month:
            average = temperature_sum / temperature_count
            print(f'{current_month}\t{average:.2f}')
            months_processed.add(current_month)
        
        end_time = time.time()
        end_memory = get_memory_usage()
        
        # Performance metrics to stderr
        sys.stderr.write(f'REDUCER PERFORMANCE:\n')
        sys.stderr.write(f'Total lines processed: {lines_processed}\n')
        sys.stderr.write(f'Lines with errors: {error_lines}\n')
        sys.stderr.write(f'Unique months processed: {len(months_processed)}\n')
        sys.stderr.write(f'Execution time: {end_time - start_time:.4f} seconds\n')
        sys.stderr.write(f'Memory usage - Start: {start_memory:.2f} MB, End: {end_memory:.2f} MB\n')
        sys.stderr.write(f'Peak memory usage: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024:.2f} MB\n')
        
    except Exception as fatal_error:
        sys.stderr.write(f'Fatal reducer error: {str(fatal_error)}\n')

if __name__ == '__main__':
    reducer()