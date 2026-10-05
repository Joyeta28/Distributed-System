import multiprocessing as mp
import os

documents = [
    "distributed systems are fun",
    "map reduce is a programming model",
    "the master splits input into map tasks",
    "reduce tasks sum the counts from map tasks",
    "fun fun fun distributed computing",
]

num_reducers = 3

def map_tasks(doc_id,text):
    print(f"[map worker {os.getpid()} is mapping dox {doc_id}:{text}]")
    pairs = []
    for word in text.split():
        pairs.append((word,1))
    return pairs

def shuffle(all_map_results,num_reducers):
    groups = {}
    for pairs in all_map_results:
        for word,count in pairs:
            groups.setdefault(word,[]).append(count)
    buckets = [{} for _ in range(num_reducers)]

    for word,count in groups.items():
        bucket_id  = hash(word)%num_reducers
        buckets[bucket_id][word] = count
    return buckets

def reduce_task(bucket_id,bucket):
    print(f"[reduce worker {os.getpid()}] reducing bucket {bucket_id}"
          f"{len(bucket)} distict words")
    result = {}
    for word,count in bucket.items():
        result[word] = sum(count)
    return result



def main():
    print("[Master] splitting input into ",len(documents),"map tasks")

    with mp.Pool(processes=4) as pool:
        map_results = pool.starmap(
            map_task,[(i,doc) for i,doc in enumerate(documents)]
        )

    buckets = shuffle(map_results,num_reducers)

    with mp.Pool(processes=num_reducers) as pool:
        reduce_results = pool.starmap(
            reduce_task,list(enumerate(buckets))
        )

    final_counts = {}
    for partial in reduce_results:
        final_counts.update(partial)
    print("[Master] final word count")

    for word,count in sorted(final_counts.items(),key=lambda kv : -kv[1]):
        print(f"{word} {count}")
    
if __name__ == "__main__":
    main()
