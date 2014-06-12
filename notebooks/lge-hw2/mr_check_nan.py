from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, RawValueProtocol, JSONProtocol, PickleProtocol
import numpy as np

class MRCheckNan(MRJob):
  INPUT_PROTOCOL = JSONProtocol

  def mapper(self, key, vec):
    vec = np.array(vec)
    for x in vec:
      if x == np.nan:
        yield key, vec

if __name__ == '__main__':
  MRCheckNan.run()
