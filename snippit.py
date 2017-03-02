# Create local array to send back
x = NP.zeros(1, dtype=npydt)
# Create array where x gathers back to
x_all = NP.zeros(size, dtype=npydt)

# Create a local array with numpy of the data type we already created
length = NP.int64(len(temp_job_events_time_calculated))
filler = NP.int64(1)
NP.put(x,0,[(length,filler,filler,filler)])

# Get the lengths of each node
comm.Barrier()
comm.Gatherv([x,mpidt], [x_all,mpidt], root=0)
# Send the lengths to each to set up the correct Gatherv later
comm.Bcast([x_all,mpidt], root=0)

# Set up the send count for each node 
for i, item in enumerate(x_all):
  individual_send_count = item[0]
  send_count[i] = individual_send_count
  # Save the count of calculated jobs
  calculated_jobs_count += individual_send_count

# Set up the displacements
displacements = NP.zeros(size, dtype="int")
displacements[0] = 0
for i in range(1,size):
  displacements[i] = displacements[i-1] + send_count[i-1]

# Resize and Zero out x
length = len(temp_job_events_time_calculated)
x = NP.zeros(length, dtype=npydt)

# Store all of the items to send into x_all
i = 0
for key in temp_job_events_time_calculated.keys():
  job_id = NP.int64(key)
  event_type = NP.int64(temp_job_events_time_calculated[key][1])
  run_time = NP.int64(temp_job_events_time_calculated[key][0])
  NP.put(x,i,[( job_id, event_type, run_time, NP.int64(i) )])
  i += 1

# Zero out x all 
x_all = NP.zeros(calculated_jobs_count, dtype=npydt)

comm.Barrier()
# Gather to root zero
comm.Gatherv([x,mpidt], [x_all, tuple(send_count), tuple(displacements), mpidt])

# Save the gathered elements into the calculated dict
if rank == 0:
  for i, item in enumerate(x_all):
    job_id = item[0]
    event_type = item[1]
    run_time = item[2]
    job_events_calculated[job_id] = [event_type, run_time]
