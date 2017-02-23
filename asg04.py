
#!/usr/bin/env python
# asg04.py

#Imports#
from mpi4py import MPI

#Globals#
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
name = MPI.Get_processor_name()

#Main#
def main():
  data = None
  newData = None
  data = init(data)
  data = scatter(data)
  newData = gather(data)

#Methods#
def init(data):
  if rank == 0:
    data = [x for x in range(size)]
    #data = [(x+1)**x for x in range(size)]
    print ("we will be scattering: %s" % (data))
  else:
    data = None
  return data

def scatter(data):
  data = comm.scatter(data, root=0)
  data = rank * rank
  print ("rank: %s has data: %s" % (rank, data))
  return data

def gather(data):
  newData = comm.gather(data, root=0)
  if rank == 0:
    print("master collected: ", newData)
    return newData

##########################
if __name__ == "__main__":
  main()
##########################