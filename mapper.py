#!/usr/bin/env python3
# mapper.py with performance tracking

import sys
import time
import resource
import logging
from datetime import datetime

def get_memory_usage():
    """Get current memory usage in megabytes"""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024

def mapper():
    """
    Input format: date,temperature
    Example: 2024-01-01,23.5
    Includes performance tracking
    """
    start_time = time.time()
    start_memory = get_memory_usage()
    
    lines_processed = 0
    error_lines = 0
    
    try:
        for line in sys.stdin:
            try:
                line = line.strip()
                if not line:  
                    continue
                    
                date, temperature = line.split(',')
                month = date.split('-')[1] 
                temperature = float(temperature)
                print(f'{month}\t{temperature}')
                
                lines_processed += 1
            except Exception as e:
                sys.stderr.write(f'Error processing line: {line}, Error: {str(e)}\n')
                error_lines += 1
                continue
        
        end_time = time.time()
        end_memory = get_memory_usage()
        
        # Performance metrics to stderr
        sys.stderr.write(f'MAPPER PERFORMANCE:\n')
        sys.stderr.write(f'Total lines processed: {lines_processed}\n')
        sys.stderr.write(f'Lines with errors: {error_lines}\n')
        sys.stderr.write(f'Execution time: {end_time - start_time:.4f} seconds\n')
        sys.stderr.write(f'Memory usage - Start: {start_memory:.2f} MB, End: {end_memory:.2f} MB\n')
        sys.stderr.write(f'Peak memory usage: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024:.2f} MB\n')
        
    except Exception as fatal_error:
        sys.stderr.write(f'Fatal mapper error: {str(fatal_error)}\n')

if __name__ == '__main__':
    mapper()