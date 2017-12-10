import json
import requests
import time
import threading
import thread

lock = threading.Lock()
from pprint import pprint

def ip_to_url(ip, location):
    return "http://" + ip + "/v2/keys/" + location

class Job:
    jobs = {}
    jobs["waiting"] = []
    jobs["running"] = []
    jobs["finished"] = []
    jobs["canceled"] = []

    operating = False
    master = "127.0.0.1:2379"

    def set_master(master):
        Job.master = master

    def __init__(self, identifier, image_url):
        self.identifier = identifier
        self.image_url = image_url

    def __str__(self):
        return self.to_json()

    def set_index(self, index):
        self.index = index

    def fresh(self, state):
        Job.jobs[self.status].remove(self)
        Job.jobs[state].append(self)
        self.status = state

    def to_json(self):
        return '{ "identifier" : "' + self.identifier + '" ,' + \
                '"image_url" : "' + self.image_url + '",' + \
                '"status" : "' + self.status + '" }'


    # Entry
    def add_job(self):
        with lock:
            self.status = "waiting"
            re = requests.put(ip_to_url(Job.master, "job/" + self.identifier), params={'value': self.to_json()})
            if re.status_code == 201:
                Job.jobs[self.status].append(self)
                print "Created " + self.identifier
            else:
                self.status = ""
                print "Cannot create this job on etcd"

    def assign_job(self, node):
        with lock:
            if self.status == "waiting":
                self.status = "running"
                self.begin_time = time.time()
                self.node = node
                re = requests.put(ip_to_url(Job.master, "job/" + self.identifier), params={'value': self.to_json()})
                if re.status_code == 200:
                    self.status = "waiting"
                    self.fresh("running")
                    print "Assigned " + self.identifier
                    return True
                else:
                    print "Cannot assign and try again"
                    self.status = "waiting"
                    self.begin_time = 0
                    self.node = ""
                    return False
            else:
                print "Must from waiting to running"


    def finish_job(self):
        with lock:
            if self.status == "running":
                self.status = "finished"
                self.finish_time = time.time()
                re = requests.put(ip_to_url(Job.master, "job/" + self.identifier), params={'value': self.to_json()})
                if re.status_code == 200:
                    self.status = "running"
                    self.fresh("finished")
                    print "Finished " + self.identifier
                    return True
                else:
                    print "Cannot finish on etcd and try again"
                    self.status = "running"
                    self.finish_time = 0
                    return False
            else:
                print "Must from running to finished"
                return False

    def cancel_job(self):
        with lock:
            status = self.status
            self.status = "canceled"
            re = requests.put(ip_to_url(Job.master, "job/" + self.identifier), params={'value': self.to_json()})
            if re.status_code == 200:
                self.status = status
                self.fresh("canceled")
                print "Canceled " + self.identifier
                return True
            else:
                print "Cannot cancel on etcd and try again"
                self.status = status
                return False

    def node_failure(self):
        with lock:
            if self.status == "running":
                self.status = "waiting"
                node = self.node
                self.node = ""
                re = requests.put(ip_to_url(Job.master, "job/" + self.identifier), params={'value': self.to_json()})
                if re.status_code == 200:
                    self.status = "running"
                    self.fresh("waiting")
                    print "Waited"
                    return True
                else:
                    self.status = "running"
                    self.node = node
                    print "Cannot back out and try again"
                    return False
            else:
                print "It makes no change"
                return True

# Do not see the code of Node
# This just for convenience
# The nodes are not reliable always so
# this class just used for demo here
class Node:
    nodes = {}
    max_job_count = 2

    def __init__(self, ip, identifier):
        self.identifier = identifier
        self.ip = ip
        self.job = []
        self.jobs_allowed = Node.max_job_count

    def to_json(self):
        return '{ "identifier": "' + self.identifier + '", ' +\
                '"ip": "' + self.ip + '" ,' + \
                '"job": "' + str(self.job) + '",' + \
                '"jobs_allowed": "' + str(self.jobs_allowed) + '" }'

    def add_node(self, master):
        re = requests.put(ip_to_url(master, "node/" + self.identifier), params={'value': str(self.to_json())})
        print re.status_code


    def finish_job(self, job):
        self.job.remove(job)
        self.jobs_allowed += 1
        re = requests.put(ip_to_url(self.ip, "finished/" + job.identifier), params={'value': str(job.to_json())})


# def Assign(node, job, master):
#     node["job"] += json.loads(node["job"]) + [job.to_json()]
#     node["jobs_allowed"] = int(node["jobs_allowed"]) - 1
#     node = json.dumps(node)
#     # re = requests.put(ip_to_url(node.ip, "jobs"), params={'value': str(job.to_json())})
#     re = requests.put(ip_to_url(master, "node/" + node["identifier"]), params={'value': node})

def GetNodes(master):
    re = requests.get(ip_to_url(master, "node"))

    nodes = json.loads(re.content)

    nodes = nodes["node"]["nodes"]#["value"]
    return nodes

def GetJobs(master):
    re = requests.get(ip_to_url(master, "job"))

    nodes = json.loads(re.content)

    nodes = nodes["node"]["nodes"] #["value"]
    return nodes

def Check(master, node):
    re = requests.get(ip_to_url(node.ip, "jobs"))
    if re.status_code == 200:
        return True
    else:
        dre = requests.get(ip_to_url(master, "node") + "/" + node.index)
        if dre.status_code == 200:
            requests.delete(ip_to_url(master, "node") + "/" + node.index)
            print "deleted node " + node.index
        else:
            print dre.content
        return False


def Scheduler(master):
    while True:
        time.sleep(2)
        jobs = Job.jobs["running"]
        # for job in jobs:
            # if not(Check(master, job.node)):
            #     job.node_failure()
            # if Finish(job.node, job)
            #     job.finish_job()
        jobs = Job.jobs["waiting"]
        nodes = GetNodes(master)
        if len(jobs) != 0:
            pos = 0
            for node in nodes:
                node = json.loads(node["value"])
                jobs_allowed = int(node["jobs_allowed"])
                # print jobs_allowed
                for job in jobs[pos:pos + jobs_allowed]:
                    if job.assign_job(node):
                        print "Schdule " + job.identifier + " to " + node["identifier"]
                        # Assign(node,job,master)
                pos += jobs_allowed






def DeleteNode(index):
    re = requests.delete(ip_to_url("127.0.0.1:2379", 'node' + index))
    # print re.content

def DeleteJob(index):
    re = requests.delete(ip_to_url("127.0.0.1:2379", 'job' + index))
    # print re.content

def main():

    master = "127.0.0.1:2379"

    node1 = Node("127.0.0.110:2379", "node1")
    node2 = Node("127.0.0.111:2379", "node2")
    node3 = Node("127.0.0.112:2379", "node3")
    node4 = Node("127.0.0.113:2379", "node4")
    node5 = Node("127.0.0.114:2379", "node5")


    job1 = Job("job1", "127.0.0.1")
    job2 = Job("job2", "127.0.0.1")
    job3 = Job("job3", "127.0.0.1")
    job4 = Job("job4", "127.0.0.1")
    job5 = Job("job5", "127.0.0.1")

    node1.add_node(master)
    node2.add_node(master)
    node3.add_node(master)
    node4.add_node(master)
    node5.add_node(master)

    job1.add_job()
    job2.add_job()


    thread.start_new_thread(Scheduler, (master, ))
    #
    time.sleep(4)
    job4.add_job()
    time.sleep(1)
    job3.add_job()
    time.sleep(5)
    job5.add_job()


    jobs = GetJobs(master)
    for job in jobs:
        print job["value"]
    nodes  =  GetNodes(master)
    for node in nodes:
        print node["value"]

    time.sleep(10)

    for i in range(1,6):
        DeleteJob("/job" + str(i))
        DeleteNode("/node" + str(i))

    




main()
