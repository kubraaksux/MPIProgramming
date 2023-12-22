from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if rank == 0:
    # Master process
    final_product = ""
    while True:
        data_probe = comm.iprobe(source=1, tag=0)
        if data_probe:
            data = comm.recv(source=1, tag=0)
            print(f"Master received {data}")
            if data == "99":  # Example of a termination message
                break
            print("Waiting")
    print("Master is done with receiving")
else:
    # Slave processes
    n=0
    for i in range(5):  # Example range for sending messages
        n += 1
        comm.send(str(i), dest=0, tag=0)
    # Optionally send a termination message
    comm.send("99", dest=0, tag=0)  # Make sure this matches the master's termination condition

    print(f"Slave {rank} is done with sending")
