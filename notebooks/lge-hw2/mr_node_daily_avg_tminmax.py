
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, JSONProtocol, PickleProtocol
import numpy as np
from os.path import basename

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

class MRNodeDailyAvgTminmax(MRJob):
  INPUT_PROTOCOL = JSONProtocol
  INTERNAL_PROTOCOL = PickleProtocol
  OUTPUT_PROTOCOL = JSONProtocol

  def mapper_init(self):
    self.station_to_node_table = StationToNodeTable()
    self.station_to_node_table.read_from_csv_file(basename(self.options.station_to_node))

  def mapper(self, key, vec):
    station, year = key.split(':')

    leaf = self.station_to_node_table.find_node(station)

    vec = np.array(vec)
    avg_tmin = np.mean(vec[:365])
    avg_tmax = np.mean(vec[366:])

    # Yield vec to the corresponding leaf node and all of its
    # ancestors.
    level = self.options.num_levels
    while level >= 0:
      node = get_node_ancestor_at_level(leaf, level)
      level -= 1
      yield node, (avg_tmin, avg_tmax)

  def reducer(self, node, vals):
    avg_tmin_vec, avg_tmax_vec = [], []
    for val in vals:
      avg_tmin, avg_tmax = val
      avg_tmin_vec.append(avg_tmin)
      avg_tmax_vec.append(avg_tmax)

    yield node, (avg_tmin_vec, avg_tmax_vec)

  def configure_options(self):
    super(MRNodeDailyAvgTminmax, self).configure_options()

    self.add_passthrough_option(
      '--num-levels',
      type='int',
      dest='num_levels',
      default=9,
      help='Number of total partition levels. Default is 9.')

    self.add_file_option(
      '--station-to-node',
      default='data/station-to-node.csv',
      help='The csv file describes the mapping between station-id to node-id.')

if __name__ == '__main__':
  MRNodeDailyAvgTminmax.run()
