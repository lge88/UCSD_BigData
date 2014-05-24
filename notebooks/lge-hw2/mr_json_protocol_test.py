
from sys import stderr
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, JSONProtocol

def vec_mean(vecs):
  out, n = None, 0
  for vec in vecs:
    if out is None:
      out = map(int, vec)
    else:
      for i, x in enumerate(vec):
        out[i] += int(x)
    n += 1

  for i, x in enumerate(out):
    out[i] = float(x) / n

  return out

class MRJSONProtocolTest(MRJob):
  INPUT_PROTOCOL = JSONProtocol

  def mapper(self, key, vec):
    station, year = key.split(':')
    yield station, vec

  def reducer(self, station, vecs):
    yield station, vec_mean(vecs)

if __name__ == '__main__':
  MRJSONProtocolTest.run()
