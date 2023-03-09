from itertools import zip_longest

def grouper(n, iterable, fillvalue=None):
    return zip(*([iter(iterable)] * n))

def batched_generator(reader, batch_size = 32):
    batch = []
    counter = 0
    itr = iter(reader)
    while True:
        try:
            batch.append(next(itr))
            counter +=1
            if counter >= batch_size:
                counter = 0
                yield batch
                batch = []
        except StopIteration as e:
            if batch:
                yield batch
            break
