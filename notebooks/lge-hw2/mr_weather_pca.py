from sys import stderr
from os import remove
from os.path import basename
import pickle
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, RawValueProtocol, JSONProtocol, PickleProtocol
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

  TYPES = {
    'mu': 0,
    'centered_vector': 1
  }

  def debug(self, obj):
    if not self.options.no_debug: stderr.write(str(obj) + '\n');

  def _debug_options(self):
    self.debug('--station-to-node: ')
    self.debug(self.options.station_to_node)

    self.debug('--no-debug: ')
    self.debug(self.options.no_debug)

    self.debug('--reduced-dimension: ')
    self.debug(self.options.reduced_dimension)

    self.debug('--explained-variance-ratio-threshold: ')
    self.debug(self.options.explained_variance_ratio_threshold)

    self.debug('--num-levels: ')
    self.debug(self.options.num_levels)

    self.debug('--store-mu: ')
    self.debug(self.options.store_mu)

    self.debug('--store-eigen-vectors: ')
    self.debug(self.options.store_eigen_vectors)

    self.debug('--station-to-node: ')
    self.debug(self.options.station_to_node)

  def node_mapper_init(self):
    self._debug_options()
    self.station_to_node_table = StationToNodeTable()
    self.station_to_node_table.read_from_csv_file(basename(self.options.station_to_node))

  def node_mapper(self, key, vec):
    # self.debug('in mapper 1, key ' + key)

    station, year = key.split(':')
    leaf = self.station_to_node_table.find_node(station)

    vec = np.array(vec)

    # Yield vec to the corresponding leaf node and all of its
    # ancestors.
    level = self.options.num_levels
    # level = MRPCA.NUM_OF_LEVELS
    while level >= 0:
      node = get_node_ancestor_at_level(leaf, level)
      level -= 1
      if self.options.reduced_dimension:
        yield node, vec[:self.options.reduced_dimension]
      else:
        yield node, vec

  def node_mean_reducer(self, node, vecs):
    # self.debug('in mean reducer, node ' + node)

    mu, n = None, 0
    for vec in vecs:
      if mu is None:
        mu = vec.copy()
      else:
        mu += vec
      n += 1
    mu /= n

  def node_substract_mean_reducer(self, node, vecs):
    # self.debug('in reducer 1, node ' + node)

    # I hope vecs can be hold in memory, but no :(
    # It will complain memory problem.
    # vecs = list(vecs)
    # mu = np.mean(vecs)

    # Solution is to write to local file first:
    # Once mu is computed, read them back.
    mu, n = None, 0
    fname = 'vecs-' + node
    with open(fname, 'w') as f:
      for vec in vecs:
        if mu is None:
          mu = vec.copy()
        else:
          mu += vec
        line = ','.join(map(str, vec.tolist()))
        f.write(line + '\n')
        n += 1

    mu /= float(n)

    # Now we have mu, process the vecs:
    with open(fname, 'r') as f:
      for line in f:
        vec = np.array(map(float, line.strip().split(',')))
        vec -= mu
        yield node, (MRPCA.TYPES['centered_vector'], vec)

    if self.options.store_mu:
      yield node, (MRPCA.TYPES['mu'], mu)

    remove(fname)

  def node_descriptor_reducer(self, node, vals):
    # self.debug('in reducer 2, node ' + node)

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

    ratio = self.options.explained_variance_ratio_threshold

    ok = True
    try:
      U, D, V = svd(cov)
    except:
      ok = False

    if ok:
      k, sofar, total = 1, 0.0, sum(D)
      sofar += D[k-1]
      while sofar < ratio * total:
        k += 1
        sofar += D[k-1]

      desc_len = nsamples * k + (k + 1) * 730

      # Dimension reduced descriptor for node.
      # Only care about decriptor length
      out = [node, k, nsamples, desc_len]

      # Full descriptor, output might be huge.
      # by default is not stored. But can be enabled.
      if self.options.store_mu:
        out.append(mu.tolist())

      if self.options.store_eigen_vectors:
        out.append(D[:k].tolist())
        out.append(U[:, :k].tolist())

      yield None, out

  def steps(self):
    return [
      self.mr(mapper_init=self.node_mapper_init, mapper=self.node_mapper,
              reducer=self.node_substract_mean_reducer),
      self.mr(reducer=self.node_descriptor_reducer)
    ]

  def configure_options(self):
    super(MRPCA, self).configure_options()

    self.add_passthrough_option(
      '--no-debug',
      default=None,
      dest='no_debug',
      action='store_true',
      help='Don\'t output debug message, default is false.')

    self.add_passthrough_option(
      '--reduced-dimension',
      type='int',
      dest='reduced_dimension',
      default=None,
      help='Reduced vector dimension (full is 730) for debugging. If it is not'
      'set, use full dimension. Default is not set.')

    self.add_passthrough_option(
      '--explained-variance-ratio-threshold',
      type='float',
      dest='explained_variance_ratio_threshold',
      default=0.99,
      help='The threshold of explaine variance ration used in PCA process. Default is 0.99.')

    self.add_passthrough_option(
      '--num-levels',
      type='int',
      dest='num_levels',
      default=9,
      help='Number of total partition levels. Default is 9.')

    self.add_passthrough_option(
      '--store-mu',
      default=None,
      dest='store_mu',
      action='store_true',
      help='Store mean vector. By default mean vector is not stored')

    self.add_passthrough_option(
      '--store-eigen-vectors',
      default=None,
      dest='store_eigen_vectors',
      action='store_true',
      help='Store eigen vectors. By default eigne vectors is not stored.')

    self.add_file_option(
      '--station-to-node',
      default='data/station-to-node.csv',
      help='The csv file describes the mapping between station-id to node-id.')

if __name__ == '__main__':
  MRPCA.run()
