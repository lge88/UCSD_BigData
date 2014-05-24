
import pickle, re

def join_stations_measurement_counts(filename, stations):
  LINE_RE = '^"(.*)"\t(.*)$'
  stations['weight'] = 0.0
  with open(filename, 'r') as f:
    for line in f.readlines():
      m = re.match(LINE_RE, line.strip())
      if m is not None:
        station, counts = m.group(1), int(m.group(2))
        if station in stations.index:
          stations.loc[station, 'weight'] = counts
  return df_to_list(stations[['latitude', 'longitude', 'weight']])

def df_to_dict(df):
  out = {}
  for indx in df.index:
    item = df.ix[indx]
    out[indx] = (item.latitude, item.longitude, item.weight)
  return out

def df_to_list(df):
  out = []
  for indx in df.index:
    item = df.ix[indx]
    out.append((str(indx), float(item.latitude), float(item.longitude), int(item.weight)))
  return out

def export_pickle_data(obj, filename):
  with open(filename, 'wb') as f:
    pickle.dump(obj, f)

def export_text_data(lst, filename):
  with open(filename, 'wb') as f:
    for item in lst:
      f.write(','.join(map(str, item)));
      f.write('\n');

def load_pickle_data(filename):
  with open(filename, 'rb') as f:
    return pickle.load(f)

if __name__ == '__main__':
  s1 = load_pickle_data('stations.pkl')
  s2 = join_stations_measurement_counts('station-measurement-counts.txt', s1)
  export_text_data(s2, 'station-lat-lon-weight.csv.txt')
