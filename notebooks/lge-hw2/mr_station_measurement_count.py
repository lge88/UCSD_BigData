
from mrjob.job import MRJob
from weather_data_parser import WeatherDataParser

class MRStationMeasurementCount(MRJob):
  def mapper_init(self):
    self.parser = WeatherDataParser()

  def mapper(self, _, line):
    res = self.parser.parse_line(line)
    if res is not None:
      station, year, measurement, data, num_of_records = res
      if measurement == 'TMIN' or measurement == 'TMAX':
        yield station, 1

  def combiner(self, key, vals):
    yield key, sum(vals)

  def reducer(self, key, vals):
    yield key, sum(vals)

if __name__ == '__main__':
  MRStationMeasurementCount.run()
