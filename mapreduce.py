import collections
import itertools
import multiprocessing

class MapReduce(object):
    
    def __init__(self, map_func, reduce_func, num_workers=2):
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.pool = multiprocessing.Pool(num_workers)
    
    def partition(self, mapped_values):
        """Organize the mapped values by their key.
        Returns an unsorted sequence of tuples with a key and a sequence of values.
        """
        # partitioned_data = collections.defaultdict(list)
        # for key, value in mapped_values:
        #     partitioned_data[key].append(value)
        # return partitioned_data.items()
        partitioned_data = []
        for value in mapped_values:
            partitioned_data.append(value)
        return partitioned_data

    
    def __call__(self, inputs, chunksize=1):
        map_responses = self.pool.map(self.map_func, inputs, chunksize=chunksize)
        partitioned_data = self.partition(itertools.chain(*map_responses))
        reduced_values = self.pool.map(self.reduce_func, partitioned_data)
        return reduced_values