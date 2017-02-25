
#!/usr/bin/env python
# asg04.py

#Imports#
from mpi4py import MPI
import numpy as np

#Globals#
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
name = MPI.Get_processor_name()

#Main#
def main():
  #Numpy Data Type Create
  npydt = np.dtype([('jobID', np.int64, 1), ('starttime', np.int64, 1), ('endtime', np.int64, 1)])
  #MPI Data Type Create
  mpidt = MPI.Datatype.Create_struct([1, 1, 1], [0,8,16], [MPI.INT64_T, MPI.INT64_T, MPI.INT64_T])
  mpidt.Commit()

  #Create a local array with numpy of the data type we already created
  x = np.zeros((1,1), dtype=npydt)

  if rank == 0:
    x_all = np.zeros((size), dtype=x.dtype)
  else:
    x_all = None

  #Searching csv files
  jobs_list = []
  search(x, x_all, mpidt, jobs_list)

  print("Completed Search on rank: %d" % rank)
  #Change back to rank 0
  if rank == 4:
    print("All jobs: ")
    print(jobs_list)
  
#Methods#
def search(x, x_all, mpidt, jobs_list):
  for i in range(500):
    if (i % size) == rank:
      np.put(x,[0], [(i,size,rank)])
      #print("Setting Barrier for rank: %d" % rank)
      comm.Barrier()
      #if rank == 0:
        #print("Gathering")
      comm.Gatherv([x,mpidt], [x_all,mpidt], root=0) 
      if rank == 0:
        print("Run number: %d" % ((i/size)+1))
        #print(x_all)
        for job in np.nditer(x_all):
          jobs_list.append(job)
          print(job)


##########################
if __name__ == "__main__":
  main()
##########################
