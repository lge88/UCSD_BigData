from sys import stderr
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, JSONProtocol, PickleProtocol
import numpy as np
from numpy.linalg import svd

from geo_partition import StationToNodeTable

def mat_add_eq(m, other, factor=1.0):
  rows = len(m)
  cols = len(m[0])
  for i in range(rows):
    for j in range(cols):
      m[i][j] += other[i][j] * factor
  return m

def vec_add_eq(vec, delta, factor=1.0):
  for i, _ in enumerate(vec):
    vec[i] += delta[i] * factor
  return vec

def vec_mean(vecs):
  out, n = None, 0
  for vec in vecs:
    if out is None:
      out = vec.copy()
    else:
      out += vec
    n += 1
  out /= n
  return out

def outer_product(v1, v2):
 # compute m = v1 * v2^T (M x N)
  M = len(v1)
  N = len(v2)
  m = [None] * M
  row = [0.0] * N
  for i in range(M):
    for j in range(N):
      x = v1[i]
      y = v2[j]
      row[j] = x * y
    m[i] = row[:]
  return m

def parse_natural_int(i):
  try:
    i = int(i)
  except ValueError:
    return 0
  return i if i >= 0 else 0

def get_node_ancestor_at_level(node, level):
  level = parse_natural_int(level)
  return node[:level]

def cerr(obj):
  stderr.write(str(obj) + '\n')

class MRPCA(MRJob):
  INPUT_PROTOCOL = JSONProtocol
  INTERNAL_PROTOCOL = PickleProtocol

  station_to_node_table = None
  EXPLAINED_RATIO_THRESHOLD = 0.95
  LEVEL = 9
  DEBUG_REDUCED_VEC_DIM = None

  TYPES = {
    'mu': 0,
    'centered_vector': 1
  }

  def find_node_for_station(self, station):
    leaf = MRPCA.station_to_node_table.find_node(station)
    node = get_node_ancestor_at_level(leaf, MRPCA.LEVEL)
    return node

  def node_mapper(self, key, vec):
    cerr('in mapper, key ' + key)
    station, year = key.split(':')
    node = self.find_node_for_station(station)

    vec = np.array(vec)
    if MRPCA.DEBUG_REDUCED_VEC_DIM is not None:
      yield node, vec[:MRPCA.DEBUG_REDUCED_VEC_DIM]
    else:
      yield node, vec

  def node_substract_mean_reducer(self, node, vecs):
    # hopefully vecs can be hold in memory
    cerr('in reducer 1, node ' + node)
    vecs = list(vecs)
    nsamples = len(vecs)
    mu = vec_mean(vecs)
    yield node, (MRPCA.TYPES['mu'], mu, nsamples)

    for vec in vecs:
      vec -= mu
      yield node, (MRPCA.TYPES['centered_vector'], vec)

  def node_descriptor_reducer(self, node, vals):
    cerr('in reducer 2, node ' + node)
    cov = None
    mu = None
    vecs = []
    nsamples = 0

    for val in vals:
      if val[0] == MRPCA.TYPES['mu']:
        mu, nsamples = val[1], val[2]
      elif val[0] == MRPCA.TYPES['centered_vector']:
        vec = val[1]
        m = np.outer(vec, vec)
        nsamples += 1
        if cov is None:
          cov = m
        else:
          cov += m
          # mat_add_eq(cov, m)
    # cov = cov / nsamples
    cov /= nsamples

    # full description:
    # yield node, (mu, cov, vecs)
    # we have mu (730 x 1), cov (730 x 730) and vecs (730 x nsamples)
    # now what?
    # It seems have to use numpy, scipy stack unless I can implement
    # the following:

    # - U, s, V = svd(cov)
    # - pick k based on the explained variance
    # - eigenvecs = V[:k] (k x 730)
    # - alphas (k x nsamples) how? -> don't care, so no

    U, s, V = svd(cov)
    ratio = MRPCA.EXPLAINED_RATIO_THRESHOLD
    k, sofar, total = 0, 0.0, sum(s)
    sofar += s[k]
    while sofar < ratio*total:
      k += 1
      sofar += s[k]

    eigenvecs = U[:, :k]

    # dimension reduced description
    yield node, (k, mu.tolist(), s[:k].tolist(), eigenvecs.tolist())
    # yield node, k


  def steps(self):
    return [
      self.mr(mapper=self.node_mapper, reducer=self.node_substract_mean_reducer),
      self.mr(reducer=self.node_descriptor_reducer)
    ]

if __name__ == '__main__':
  MRPCA.station_to_node_table = StationToNodeTable()
  MRPCA.station_to_node_table.read_from_csv_file('station-to-node-table-yoav.csv')
  MRPCA.LEVEL = 0
  # MRPCA.DEBUG_REDUCED_VEC_DIM = 5
  MRPCA.run()
