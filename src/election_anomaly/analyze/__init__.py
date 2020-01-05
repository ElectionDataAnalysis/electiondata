#!usr/bin/python3


import numpy as np
from scipy import stats as stats
import scipy.spatial.distance as dist

def vector_list_to_scalar_list(li):
    """Given a list of vectors, all of same length,
    return list of scalars ,
    for each vector, sum metric distance from all other vectors of metric function
    """
    a = [sum([dist.euclidean(x,y) for x in li]) for y in li]
    return a

def euclidean_zscore(li):
    """Take a list of vectors -- all in the same R^k,
    returns a list of the z-scores of the vectors -- each relative to the ensemble"""
    return list(stats.zscore([sum([dist.euclidean(x,y) for x in li]) for y in li]))

if __name__ == '__main__':
    li = [[0,0],[100,0],[101,0],[100,2]]
    a = np.array(vector_list_to_scalar_list(li))
    print (a)
    print('stats.zscore(a):')
    print(stats.zscore(a))
    print('stats.zscore(li):')
    print(stats.zscore(li))
    print('Euclidean outlier z-score')
    print(euclidean_zscore(li))

