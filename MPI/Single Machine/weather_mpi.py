from mpi4py import MPI
import numpy as np
import time
import psutil
import os
import json
from datetime import datetime

class PerformanceMonitor:
    def __init__(self):
        self.start_time = None
        self.start_cpu_times = None
        self.start_memory = None
        
    def start(self):
        self.start_time = time.time()
        self.start_cpu_times = psutil.Process().cpu_times()
        self.start_memory = psutil.Process().memory_info().rss
        
    def end(self):
        end_time = time.time()
        end_cpu_times = psutil.Process().cpu_times()
        end_memory = psutil.Process().memory_info().rss
        
        metrics = {
            'wall_time': end_time - self.start_time,
            'user_cpu_time': end_cpu_times.user - self.start_cpu_times.user,
            'system_cpu_time': end_cpu_times.system - self.start_cpu_times.system,
            'total_cpu_time': (end_cpu_times.user - self.start_cpu_times.user) + 
                            (end_cpu_times.system - self.start_cpu_times.system),
            'memory_used': (end_memory - self.start_memory) / 1024 / 1024,  # MB
            'cpu_percent': psutil.Process().cpu_percent(),
            'hostname': os.uname().nodename
        }
        return metrics

def process_weather_data(data_size=100000):
    """Process weather data with performance monitoring"""
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    monitor = PerformanceMonitor()
    monitor.start()
    
    # Generate or receive data
    if rank == 0:
        # Generate sample data
        data = []
        for i in range(data_size):
            month = (i % 12) + 1
            temp = 20 + np.random.normal(0, 5)
            data.append((month, temp))
        data = np.array(data)
        data_generation_time = time.time() - monitor.start_time
    else:
        data = None
        data_generation_time = 0
    
    # Broadcast data size
    if rank == 0:
        chunk_size = len(data) // size
        remainder = len(data) % size
    else:
        chunk_size = None
        remainder = None
    
    # Synchronize processes
    comm.Barrier()
    distribution_start = time.time()
    
    # Broadcast chunk information
    chunk_size = comm.bcast(chunk_size, root=0)
    remainder = comm.bcast(remainder, root=0)
    
    # Distribute data
    if rank == 0:
        chunks = []
        start = 0
        for i in range(size):
            chunk_end = start + chunk_size + (1 if i < remainder else 0)
            chunks.append(data[start:chunk_end])
            start = chunk_end
        local_data = chunks[0]
        for i in range(1, size):
            comm.send(chunks[i], dest=i)
    else:
        local_data = comm.recv(source=0)
    
    # Synchronize after distribution
    comm.Barrier()
    distribution_time = time.time() - distribution_start
    
    # Process local data
    computation_start = time.time()
    local_results = {}
    for month, temp in local_data:
        month = int(month)
        if month not in local_results:
            local_results[month] = {'sum': 0, 'count': 0}
        local_results[month]['sum'] += temp
        local_results[month]['count'] += 1
    
    # Synchronize after computation
    comm.Barrier()
    computation_time = time.time() - computation_start
    
    # Gather results
    gathering_start = time.time()
    all_results = comm.gather(local_results, root=0)
    
    # Get performance metrics
    perf_metrics = monitor.end()
    perf_metrics.update({
        'data_generation_time': data_generation_time,
        'distribution_time': distribution_time,
        'computation_time': computation_time,
        'gathering_time': time.time() - gathering_start
    })
    all_metrics = comm.gather(perf_metrics, root=0)
    
    if rank == 0:
        # Process results
        final_results = {}
        for process_result in all_results:
            for month, data in process_result.items():
                if month not in final_results:
                    final_results[month] = {'sum': 0, 'count': 0}
                final_results[month]['sum'] += data['sum']
                final_results[month]['count'] += data['count']
        
        # Calculate performance statistics
        total_wall_time = max(m['wall_time'] for m in all_metrics)
        total_cpu_time = sum(m['total_cpu_time'] for m in all_metrics)
        avg_memory = sum(m['memory_used'] for m in all_metrics) / len(all_metrics)
        
        # Prepare performance report
        report = {
            'timestamp': datetime.now().isoformat(),
            'configuration': {
                'processes': size,
                'data_size': data_size,
                'hostname': os.uname().nodename
            },
            'overall_metrics': {
                'total_wall_time': total_wall_time,
                'total_cpu_time': total_cpu_time,
                'cpu_efficiency': (total_cpu_time/total_wall_time/size)*100,
                'avg_memory_per_process': avg_memory,
                'parallel_efficiency': (1/(total_wall_time*size))*100
            },
            'timing_breakdown': {
                'data_generation': all_metrics[0]['data_generation_time'],
                'distribution': sum(m['distribution_time'] for m in all_metrics) / len(all_metrics),
                'computation': sum(m['computation_time'] for m in all_metrics) / len(all_metrics),
                'gathering': sum(m['gathering_time'] for m in all_metrics) / len(all_metrics)
            },
            'process_metrics': all_metrics
        }
        
        # Save report
        os.makedirs('performance_reports', exist_ok=True)
        filename = f'performance_reports/report_{size}processes_{data_size}records_{int(time.time())}.json'
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Print summary
        print("\nPerformance Summary:")
        print("=" * 50)
        print(f"Configuration:")
        print(f"  Number of Processes: {size}")
        print(f"  Data Size: {data_size} records")
        print(f"\nTiming Metrics:")
        print(f"  Total Wall Time: {total_wall_time:.3f} seconds")
        print(f"  Total CPU Time: {total_cpu_time:.3f} seconds")
        print(f"  CPU Efficiency: {(total_cpu_time/total_wall_time/size)*100:.1f}%")
        print(f"\nMemory Metrics:")
        print(f"  Average Memory per Process: {avg_memory:.1f} MB")
        print(f"\nParallel Performance:")
        print(f"  Parallel Efficiency: {(1/(total_wall_time*size))*100:.1f}%")
        print(f"\nDetailed timing breakdown:")
        print(f"  Data Generation: {report['timing_breakdown']['data_generation']:.3f}s")
        print(f"  Data Distribution: {report['timing_breakdown']['distribution']:.3f}s")
        print(f"  Computation: {report['timing_breakdown']['computation']:.3f}s")
        print(f"  Result Gathering: {report['timing_breakdown']['gathering']:.3f}s")
        print("\nFull report saved to:", filename)

if __name__ == "__main__":
    process_weather_data()