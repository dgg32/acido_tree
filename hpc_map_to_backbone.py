import sys, json, os


from threading import Thread
from threading import Semaphore
import queue

writeLock = Semaphore(value=1)

in_queue = queue.Queue()

tree = json.loads(open(sys.argv[1]).read())

mapping = {}

for otu in tree:
    for member in tree[otu]["member"]:
        shortname = tree[otu]["member"][member].split(" ")[0]
        mapping[shortname] = otu

#print mapping
cutoff = 99

def work():

    while True:
        packet = in_queue.get()
        
        try:
            #query name, best id, otu list
            query_id_otu = {}
            for line in open(packet, 'r'):
                fields = line.split("\t")
                query = fields[0]
                subject = fields[1]
                identity = float(fields[2])
                #length = int(fields[3])
                #evalue = float(fields[-2])
                
                #best_id_of_query = 0

                if query not in query_id_otu:
                    query_id_otu[query] = (identity, [])

                    otu = mapping[subject]
                    if otu not in query_id_otu[query][1]:
                        query_id_otu[query][1].append(otu)
                
                else:

                    if identity == query_id_otu[query][0]:
                        otu = mapping[subject]
                        if otu not in query_id_otu[query][1]:
                            query_id_otu[query][1].append(otu)


            writeLock.acquire()
            print (json.dumps(query_id_otu))
            writeLock.release()
        except:
            pass
        finally:
            in_queue.task_done()

for i in range(70):
    t = Thread(target=work)
    t.daemon = True
    t.start()

top_folder = sys.argv[2]

for (head, dirs, files) in os.walk(top_folder):
    for file in files:
        if file.endswith(".txt"):
            current_file_path = os.path.abspath(os.path.dirname(os.path.join(head, file)))
            with_name = current_file_path + "/"+ file
            in_queue.put(with_name)
    
in_queue.join()