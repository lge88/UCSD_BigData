
from mrjob.job import MRJob

class MRCountWord(MRJob):
  def mapper(self, _, line):
    yield 'chars', len(line)
    yield 'word', len(line.split())
    yield 'lines', 1

  def reducer(self, key, values):
    yield key, sum(values)

if __name__ == '__main__':
  MRCountWord.run()
