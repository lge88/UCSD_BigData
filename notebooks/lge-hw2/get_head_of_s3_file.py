
import boto

def read_line_from_key(k):
  buf = ''
  c = k.read(1)
  count = 0
  while c != '\n' and count < 5000:
    buf += c
    count += 1
    c = k.read(1)
  return buf


if __name__ == '__main__':
  conn = boto.connect_s3('AKIAILXLBSRPY54JL24A', 'ZoDZIWV4UuxIaNwa4B/YH+RgxRRBmNlfdR2/MwBg')
  b = conn.get_bucket('weather-analysis')
  k = b.get_key('weather-json-protocol-test-2/part-00000')

  i, N = 0, 100
  while i < N:
    line = read_line_from_key(k)
    print line
    i += 1
