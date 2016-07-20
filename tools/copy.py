import struct
import os
import sys

def scan(d):
    l = []
    for f in os.listdir(d):
        if f.isdigit():
            l.append(f)
    l.sort(reverse = True)
    return l

def commitIndex(d):
    f = open(os.path.join(d, "meta"))
    s = f.read(24)
    _, _, index = struct.unpack('QQQ', s)
    return index

def main():
    if len(sys.argv) < 2:
        return
    logdir = sys.argv[1]
    index = commitIndex(logdir)
    l =  scan(logdir)

    for i in l:
        if index >= int(i):
            break
    
    indexFile = open(os.path.join(logdir, i))
    indexFile.seek(4 * (index - int(i)))
    logOffset = indexFile.read(4)
    n, = struct.unpack('I', logOffset)

    logFile = open(os.path.join(logdir, i + '.log'))
    logFile.seek(n)
    header = logFile.read(4)
    n, = struct.unpack('I', header)
    body = logFile.read(n)

    indexName = '%020d'%(index)
    logName = indexName + '.log'
    newIndexFile = open(indexName, 'w')
    newLogFile = open(logName, 'w')
    newIndexFile.write(struct.pack('I', 0))
    newLogFile.write(header + body)

    os.system('cp %s ./'%(os.path.join(logdir, 'meta')))
    print index

if __name__ == '__main__':
    main()
