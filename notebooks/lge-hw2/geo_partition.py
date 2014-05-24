
import pickle, re, random

class Station:
  def __init__(self, name, lat, lon, weight):
    self.name = str(name)
    self.lat = float(lat)
    self.lon = float(lon)
    self.weight = int(weight)

  def __repr__(self):
    return '\n'.join([
      'name: ' + str(self.name),
      'lat: ' + str(self.lat),
      'lon: ' + str(self.lon),
      'weight: ' + str(self.weight),
    ])

class StationToNodeTable:
  def __init__(self):
    self._table = {}

  def read_from_csv_file(self, fname):
    with open(fname, 'r') as f:
      for line in f.readlines():
        items = line.strip().split(',')
        if len(items) == 2:
          station, node = items
          if station and node:
            self._table[station] = node

  def size(self):
    return len(self._table)

  def find_node(self, station_name):
    return self._table[station_name] if self._table.has_key(station_name) else None

  def print_sample(self, N=10):
    ks = random.sample(self._table, N)
    for k in ks: print k, self._table[k]

def sort_stations_by_lat(stations):
  stations.sort(key=lambda x: x.lat)

def sort_stations_by_lon(stations):
  stations.sort(key=lambda x: x.lon)

def find_weighted_median_index(stations):
  total_weight = reduce(lambda sofar, item: sofar + item.weight, stations, 0)
  half_weight = total_weight / 2

  # binary search in acc array will be faster than linear search
  acc = 0
  for i in range(len(stations)):
    acc += stations[i].weight
    if (acc >= half_weight): break

  return i


# TODO: implement partition algo based on station weights
# For now, use yoav's partitioned data structure.
def partition(stations, direction='lat'):
  if direction != 'lon': direction = 'lat'
  if direction == 'lat': sort_stations_by_lat(stations)
  else: sort_stations_by_lon(stations)
  indx = find_weighted_median_index(stations)
  return (indx, stations[i], stations[:i], stations[i+1:])

class TreeNode:
  def __init__(self):
    self.parent = None
    self.stations = []
    self.children = []

  def is_leaf(self):
    return len(self.children) == 0

def write_pickle(obj, fname):
  with open(fname, 'wb') as f: pickle.dump(obj, f)

def read_pickle(fname):
  with open(fname, 'rb') as f: return pickle.load(f)

def read_csv(fname):
  with open(fname) as f:
    return [Station(*line.strip().split(',')) for line in f.readlines()]

def partition(stations):
  root = TreeNode()
  # build kd-tree from stations
  return root

if __name__ == '__main__':
  # stations = read_csv('station-lat-lon-weight.csv')
  # sort_stations_by_lat(stations)
  # print find_weighted_median(stations)

  station_to_node = StationToNodeTable()
  station_to_node.read_from_csv_file('station-to-node-table-yoav.csv')
  station_to_node.print_sample()
