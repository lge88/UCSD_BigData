
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, JSONProtocol, PickleProtocol

class MRTestNumpy(MRJob):
  INPUT_PROTOCOL = JSONProtocol
  INTERNAL_PROTOCOL = PickleProtocol

  ERR = None
  ERR_REPORTED = False

  def mapper(self, key, vec):

    A, b, r_v, p_v, std_err = linregress(a, b)
    station, year = key.split(':')
    try:
      mu = np.mean(vec)
      yield station, mu
    except Exception, e:
      yield 'error', str(e)

  def reducer(self, station, vals):
    try:
      mu = np.mean(list(vals))
      yield station, mu
    except Exception, e:
      yield 'error', str(e)

    if MRTestNumpy.ERR is not None and MRTestNumpy.ERR_REPORTED == False:
      yield 'error', str(MRTestNumpy.ERR)
      MRTestNumpy.ERR_REPORTED = True

if __name__ == '__main__':
  try:
    import numpy as np
    from scipy.stats import linregress
  except Exception, e:
    MRTestNumpy.ERR = e

  MRTestNumpy.run()
