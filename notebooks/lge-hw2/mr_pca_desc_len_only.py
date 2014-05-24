from sys import stderr
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, JSONProtocol, PickleProtocol
import numpy as np
from numpy.linalg import svd

from geo_partition import StationToNodeTable

def parse_natural_int(i):
  try:
    i = int(i)
  except ValueError:
    return 0
  return i if i >= 0 else 0

def get_node_ancestor_at_level(node, level):
  level = parse_natural_int(level)
  return node[:level]

class MRPCA(MRJob):
  INPUT_PROTOCOL = JSONProtocol
  INTERNAL_PROTOCOL = PickleProtocol
  OUTPUT_PROTOCOL = JSONValueProtocol

  EXPLAINED_RATIO_THRESHOLD = 0.95
  NUM_OF_LEVELS = 9
  REDUCED_VEC_DIM = None
  DESCRIPTOR_LENGTH_ONLY = True
  DEBUG = True
  station_to_node_table = None

  TYPES = {
    'mu': 0,
    'centered_vector': 1
  }

  def debug(self, obj):
    if MRPCA.DEBUG == True:
      stderr.write(str(obj) + '\n');

  def node_mapper(self, key, vec):
    self.debug('in mapper 1, key ' + key)

    station, year = key.split(':')
    leaf = MRPCA.station_to_node_table.find_node(station)

    vec = np.array(vec)

    # Yield vec to the corresponding leaf node and all of its
    # ancestors.
    level = MRPCA.NUM_OF_LEVELS
    while level >= 0:
      node = get_node_ancestor_at_level(leaf, level)
      level -= 1
      if MRPCA.REDUCED_VEC_DIM is not None:
        yield node, vec[:MRPCA.REDUCED_VEC_DIM]
      else:
        yield node, vec

  def node_substract_mean_reducer(self, node, vecs):
    self.debug('in reducer 1, node ' + node)

    # hopefully vecs can be hold in memory
    vecs = list(vecs)
    mu = np.mean(vecs)

    if MRPCA.DESCRIPTOR_LENGTH_ONLY == False:
      yield node, (MRPCA.TYPES['mu'], mu)

    for vec in vecs:
      vec -= mu
      yield node, (MRPCA.TYPES['centered_vector'], vec)

  def node_descriptor_reducer(self, node, vals):
    self.debug('in reducer 2, node ' + node)

    cov = None
    mu = None
    nsamples = 0

    for val in vals:
      if val[0] == MRPCA.TYPES['mu']:
        mu = val[1]
      elif val[0] == MRPCA.TYPES['centered_vector']:
        vec = val[1]
        m = np.outer(vec, vec)
        nsamples += 1
        if cov is None:
          cov = m
        else:
          cov += m
    cov /= nsamples

    U, D, V = svd(cov)
    ratio = MRPCA.EXPLAINED_RATIO_THRESHOLD
    k, sofar, total = 1, 0.0, sum(D)
    sofar += D[k-1]
    while sofar < ratio * total:
      k += 1
      sofar += D[k-1]

    desc_len = nsamples * k + (k + 1) * 730

    # Dimension reduced descriptor
    # Only care about decriptor length
    if MRPCA.DESCRIPTOR_LENGTH_ONLY == True:
      yield None, (node, k, nsamples, desc_len)
    # Full descriptor, output might be huge
    else:
      yield None, (node, k, nsamples, desc_len,
                   mu.tolist(), D[:k].tolist(),
                   U[:, :k].tolist())

  def steps(self):
    return [
      self.mr(mapper=self.node_mapper,
              reducer=self.node_substract_mean_reducer),
      self.mr(reducer=self.node_descriptor_reducer)
    ]

if __name__ == '__main__':
  MRPCA.station_to_node_table = StationToNodeTable()
  MRPCA.station_to_node_table.read_from_csv_file('station-to-node-table-yoav.csv')
  MRPCA.EXPLAINED_RATIO_THRESHOLD = 0.99
  MRPCA.NUM_OF_LEVELS = 9
  MRPCA.REDUCED_VEC_DIM = None
  MRPCA.DESCRIPTOR_LENGTH_ONLY = True
  MRPCA.DEBUG = True
  MRPCA.run()
