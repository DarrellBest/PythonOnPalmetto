
#!/usr/bin/env python
# asg04.py

#Imports#
from mpi4py import MPI
import numpy as np
import csv

#Globals#
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
name = MPI.Get_processor_name()

NUMBER_FILES = 500

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
  jobs_list = dict()

  #for i in range(500):
  search(x, x_all, mpidt, jobs_list)
  #Change back to rank 0
  #if rank == 0:
    #print("All jobs: ")
    #print(jobs_list)i

#  print("Completed Search on rank: %d" % rank)

#  comm.Barrier()
#  if rank == 0:
#    for item in jobs_list.items():
#      print(item)
#    print("size: %s excess: %d" % (size,(NUMBER_FILES % size)))


#Methods#
def search(x, x_all, mpidt, jobs_list):
  excess = NUMBER_FILES % size
  for i in range(NUMBER_FILES + excess):
    np.put(x,[0],[(-1,-1,-1)])
    if (i % size) == rank:
      if i < NUMBER_FILES:
        np.put(x,[0], [(i,size,rank)])
        #Build CSV file name
        directory_name = "/scratch3/bbest/gtrace/gtrace/job_events/"
        file_name = name_builder(i, directory_name)
        csv_file = open(file_name)
        csv_reader = csv.reader(csv_file)
        #Parse file
        if i == 1:
         parse_csv(csv_reader)
      
      comm.Barrier()
      comm.Gatherv([x,mpidt], [x_all,mpidt], root=0)
      if rank == 0:
        #print("Run number: %d" % ((i/size)+1))
        #print(x_all)
        for job in x_all:
          if job[0] != -1:
            jobs_list.update({job[0]:[job[1],job[2]]})
            #print("job elements: %s %s %s " % (job[0],job[1],job[2]))
            
def name_builder(i, directory_name):
  file_name = directory_name + "part-00"
  if i < 10:
    file_name  = file_name + "00" + str(i)
  elif i < 100:
    file_name  = file_name + "0" + str(i)
  elif i < 1000:
    file_name  = file_name + "" + str(i)
  file_name = file_name + "-of-00500.csv"
  return file_name

def parse_csv(csv_reader):
  jobs_start = dict()
  jobs_end = dict()
  for row in csv_reader:
    #If a job event is 0, which means it is starting
    if (str(row[3]) == str(0)):
      #print(row)
      jobs_start[row[2]] = [row[3], int(row[0])]
  #print(jobs)
    elif (str(row[3]) != 1):
      jobs_end[row[2]] = [row[3], int(row[0])]

  for key, value in jobs_end.items():
    print( "Key: %s  Value: %s"  % (key,value))
    if key in jobs_start.keys():
      jobs_start[key] = [value[0], value[1]-jobs_start[key][1]]
      print("%s - %s = %s" % (value[1], jobs_start[key][1], value[1]-jobs_start[key][1]))

    #print ("job id: %s event type: %s time stamp: %s"  % (row[2], row[3], row[0]))
  



##########################
if __name__ == "__main__":
  main()
##########################