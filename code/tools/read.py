def by_line(file):
    with open(file, 'r') as fh:
        return fh.read().strip().split('\n')

