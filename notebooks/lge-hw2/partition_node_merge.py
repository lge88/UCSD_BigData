class Node:
  def __init__(self, id, k, nsamples, desc_len, coord='', thres=0.0, should_merge=False):
    self.id = id
    self.k = int(k)
    self.nsamples = int(nsamples)
    self.desc_len = int(desc_len)
    self.coord = str(coord)
    self.thres = float(thres)
    self.should_merge = bool(should_merge);
    self.group = 0;

  def __repr__(self):
    return self.to_csv_line()

  def level(self):
    return len(self.id)

  def left_child(self, nodes_dict):
    id = self.id + '0'
    return nodes_dict[id] if nodes_dict.has_key(id) else None

  def right_child(self, nodes_dict):
    id = self.id + '1'
    return nodes_dict[id] if nodes_dict.has_key(id) else None

  def parent(self, nodes_dict):
    id = self.id[:-1]
    return nodes_dict[id] if nodes_dict.has_key(id) else None

  def to_csv_line(self):
    l = map(str, [
      self.id,
      self.coord,
      self.thres,
      self.k,
      self.nsamples,
      self.desc_len,
      self.group
      # '1' if self.should_merge else '0'
    ])
    return ','.join(l)

def print_all_nodes(nodes_dict):
  for k, v in nodes_dict.items():
    print k, v

def read_node_descriptors_from_csv(fname):
  nodes_dict = {}
  with open(fname) as f:
    for line in f:
      id, k, nsamples, desc_len = line.split(',')
      nodes_dict[id] = Node(id, k, nsamples, desc_len)
  return nodes_dict

def join_node_spatial_info_from_csv(nodes_dict, fname):
  with open(fname) as f:
    for line in f:
      id, coord, thres = line.split(',')
      if nodes_dict.has_key(id):
        nodes_dict[id].coord = coord
        nodes_dict[id].thres = float(thres)
  return nodes_dict

def write_nodes_to_csv(nodes_dict, fname):
  with open(fname, 'w') as f:
    for _, n in nodes_dict.items():
      f.write(n.to_csv_line())
      f.write('\n')

def compute_whether_children_of_nodes_should_be_merged_at_level(nodes_dict, level):
  nodes = [v for k, v in nodes_dict.items() if v.level() == level]

  merge_count = 0
  total_count = 0

  for n in nodes:
    total_count += 1
    lc, rc = n.left_child(nodes_dict), n.right_child(nodes_dict)
    if lc and rc and lc.desc_len + rc.desc_len > n.desc_len:
      merge_count += 1
      lc.should_merge = True
      rc.should_merge = True
  print level, merge_count, '/', total_count
  return nodes_dict

def compute_nodes_should_be_merged(nodes_dict, num_levels):
  for i in range(num_levels + 1):
    compute_whether_children_of_nodes_should_be_merged_at_level(nodes_dict, i)
  return nodes_dict

def compute_nodes_groups(nodes_dict):
  nodes = [v for k, v in nodes_dict.items()]

  for node in nodes:
    nid = node.parent(nodes_dict).left_child(nodes_dict).id if node.should_merge else node.id
    gid = int(nid, 2) + 1 if nid != '' else 0
    node.group = gid


if __name__ == '__main__':
  fin_name_1 = 'data/node-descriptor-k-n-dl-1-of-100.csv'
  fin_name_2 = 'data/partition-tree-yoav.csv'
  fout_name = 'data/partition-tree-nid-coord-thres-k-n-dl-gid-1-of-100.csv'
  num_levels = 9

  nodes_dict = read_node_descriptors_from_csv(fin_name_1)
  join_node_spatial_info_from_csv(nodes_dict, fin_name_2)
  compute_nodes_should_be_merged(nodes_dict, num_levels)
  compute_nodes_groups(nodes_dict)

  write_nodes_to_csv(nodes_dict, fout_name)
