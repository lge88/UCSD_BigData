
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol, JSONProtocol, PickleProtocol
import numpy as np
from scipy.stats import kstest

class MRKSTestMerge(MRJob):
  INPUT_PROTOCOL = JSONProtocol
  INTERNAL_PROTOCOL = PickleProtocol
  OUTPUT_PROTOCOL = RawValueProtocol

  def mapper(self, node, vecs):
    avg_tmin_vec, avg_tmax_vec = vecs

    vec = [node]
    vec.append(str(len(avg_tmin_vec)))
    vec.append(str(len(avg_tmax_vec)))
    vec += map(str, avg_tmin_vec)
    vec += map(str, avg_tmax_vec)

    line = ','.join(vec)

    yield None, line

if __name__ == '__main__':
  MRKSTestMerge.run()
