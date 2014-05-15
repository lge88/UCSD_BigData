
from mrjob.job import MRJob

class MRCountWord(MRJob):
  def mapper(self, _, line):
    yield 'lines', 1

  def reducer(self, key, values):
    yield key, sum(values)

if __name__ == '__main__':
  MRCountWord.run()
