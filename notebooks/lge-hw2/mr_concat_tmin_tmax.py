
from mrjob.job import MRJob
from sys import stderr
from weather_data_parser import WeatherDataParser

class MRConcatTminTmax(MRJob):

  parser = None

  def parse_line(self, line):
    return MRConcatTminTmax.parser.parse_line(line)

  def concat_tmin_tmax_mapper(self, _, line):
    res = self.parse_line(line)

    # stderr.write('line: ' + line + '\n')
    if res is not None:
      station, year, measurement, vec, nr = res
      if measurement == 'TMAX' or measurement == 'TMIN':
        key = station + ':' + str(year)
        # stderr.write('key: ' + key + '\n')
        stderr.write('key: ' + key + ' n: ' + str(nr) + '\n')
        # self.increment_counter('mapper', station)
        yield key, (measurement, vec)

  def concat_tmin_tmax_reducer(self, key, vals):
    out = None
    vals = list(vals)
    # stderr.write('key: ' + key + ' vals: ' + str(vals) + '\n')
    for v in vals:
      if out is None:
        out = v[1]
      else:
        if v[0] == 'TMIN':
          out = v[1] + out
        else:
          out = out + v[1]
        stderr.write(key + ' is done.\n')
        yield key, out

  def steps(self):
    return [
      self.mr(mapper=self.concat_tmin_tmax_mapper,
              reducer=self.concat_tmin_tmax_reducer)
    ]

if __name__ == '__main__':
  MRConcatTminTmax.parser = WeatherDataParser()
  MRConcatTminTmax.run()
