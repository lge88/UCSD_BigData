
from sys import stderr
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol, JSONProtocol, PickleProtocol

class MRWeatherAvgTemp(MRJob):
  INPUT_PROTOCOL = JSONProtocol
  INTERNAL_PROTOCOL = PickleProtocol
  OUTPUT_PROTOCOL = RawValueProtocol

  ERR = None
  ERR_REPORTED = False

  def debug(self, x):
    stderr.write(str(x) + '\n')

  def parse_year(self, x):
    try:
      year = int(x)
    except ValueError:
      return None

    if year < 0 or np.isnan(year): return None
    return year

  def mapper(self, key, tminmax_vec):

    station, year = key.split(':')
    year = self.parse_year(year)
    avg_temp = np.mean(tminmax_vec)

    if year is not None:
      yield station, (year, avg_temp)
      # try:
      #   yield station, (year, avg_temp)
      # except Exception, e:
      #   yield 'error', str(e)

  def reducer(self, station, vals):

    x, y = [], []
    for year, avg_temp in vals:
      x.append(year)
      y.append(avg_temp)

    # Make sure the linear fit is meaningful:
    if len(x) > 2:
      A, b, r_v, p_v, std_err = linregress(x, y)

      record = map(str, [station, A, b, r_v, p_v, std_err])
      record = ','.join(record)

      try:
        yield None, record
      except Exception, e:
        yield 'error', str(e)

    if MRWeatherAvgTemp.ERR is not None and MRWeatherAvgTemp.ERR_REPORTED == False:
      yield 'error', str(MRWeatherAvgTemp.ERR)
      MRWeatherAvgTemp.ERR_REPORTED = True

if __name__ == '__main__':
  try:
    import numpy as np
    from scipy.stats import linregress
  except Exception, e:
    MRWeatherAvgTemp.ERR = e

  MRWeatherAvgTemp.run()
