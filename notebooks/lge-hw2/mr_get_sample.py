
from sys import stderr
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from random import random

class MRGetSample(MRJob):
  OUTPUT_PROTOCOL = RawValueProtocol

  def configure_options(self):
    super(MRGetSample, self).configure_options()
    self.add_passthrough_option(
      '--probability',
      type='float',
      default=0.01,
      help='Sample probability')

  def mapper(self, _, line):
    if random() < self.options.probability:
      yield None, line

  def reducer(self, _, lines):
    for line in lines:
      yield None, line

if __name__ == '__main__':
  MRGetSample.run()
