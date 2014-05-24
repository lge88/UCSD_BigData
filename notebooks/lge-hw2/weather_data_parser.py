
class WeatherDataParser:
  DAYS = 365

  def __init__(self,
               default_int_val = None,
               good_data_ratio_threshold = 0.5,
               enable_post_processing = True,
               selected_days = None):
    self.default_int_val = default_int_val
    self.good_data_ratio_threshold = good_data_ratio_threshold
    self.enable_post_processing = enable_post_processing

    if selected_days is None:
      self.selected_days = range(WeatherDataParser.DAYS)
    else:
      self.selected_days = set([day for day in selected_days if day >= 0 and day < WeatherDataParser.DAYS])

  def parse_int(self, x):
    try:
      return int(x)
    except ValueError:
      return self.default_int_val

  def post_process_data(self, data):
    def find_nearest_neighbor(data, indx):
      offset, days = 1, WeatherDataParser.DAYS
      half_days = days / 2
      while offset <= half_days:
        r_indx = (indx + offset) % days
        val = data[r_indx]
        if val is not None: return val

        l_indx = (indx - offset) % days
        val = data[l_indx]
        if val is not None: return val

        offset += 1
      return self.default_int_val
      # raise Exception('Should never reach hear with proper good data ratio threshold!')

    for i, x in enumerate(data):
      if x is None:
        data[i] = find_nearest_neighbor(data, i)

  def parse_line(self, line):
    vec = line.strip().split(',')
    station = vec[0]
    measurement = vec[1]
    year = self.parse_int(vec[2])

    data, num_of_records = [], 0

    for day in self.selected_days:
      item = vec[3 + day]
      x = self.parse_int(item)
      if x is not None: num_of_records += 1
      data.append(x)

    num_of_days = len(self.selected_days)

    if num_of_days == 0 or float(num_of_records) / num_of_days < self.good_data_ratio_threshold:
      return None
    else:
      if self.enable_post_processing: self.post_process_data(data)
      return (station, year, measurement, data, num_of_records)

if __name__ == '__main__':
  wdp = WeatherDataParser(selected_days=range(20))

  with open('./weather-923-of-9358395-shuffled.csv') as f:
    print 1, wdp.parse_line(f.readline())
    print 2, wdp.parse_line(f.readline())
    print 3, wdp.parse_line(f.readline())
    print 4, wdp.parse_line(f.readline())
    print 5, wdp.parse_line(f.readline())
