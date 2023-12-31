from mpi4py import MPI
import numpy as np

#WINDOWS
#https://www.microsoft.com/en-us/download/details.aspx?id=105289
#mpiexec

#You may also check:
#https://www.mpich.org/downloads/

#Ubuntu (I didn't check)
#sudo apt install openmpi-bin openmpi-dev openmpi-common openmpi-doc libopenmpi-dev
#mpicc --showme:version

#MacOS (I didn't check)
#brew install open-mpi
#mpicc --showme:version


#mpi4py documentation
#https://mpi4py.readthedocs.io/en/stable/

#MPI Tutorial
#https://mpitutorial.com/tutorials/





#Initilazing MPI environment
comm = MPI.COMM_WORLD


size = comm.Get_size()
rank = comm.Get_rank()


if rank == 0:
    print(f"I am the master, I have rank {rank}")
    print(f"There are {size} processes running")
    for i in range(1, size):
        comm.send(42, dest = i, tag = 1)
else:
    print(f"I am a slave, I have rank {rank}")
    received_int = None
    received_int = comm.recv(source = 0, tag = MPI.ANY_TAG)
    #comm.Irecv(received_int, source = 0, tag = MPI.ANY_TAG)
    print(f"My master blessed me with integer {received_int}")



if rank == 0:
    data = np.arange(20, dtype = int)
    comm.Send([data, MPI.INT], dest = 1, tag = 2)
    print(rank, data)
else:
    data = np.empty(20, dtype = int)
    comm.Recv([data, MPI.INT], source = 0, tag = 2)
    print(rank, data)



if rank == 0:
    data = "first"
    comm.send(data, dest = 1, tag = 0)
    print(rank, data)
    data = "second"
    comm.send(data, dest = 1, tag = 1)
    print(rank, data)
else:
    data = comm.recv(source = 0, tag = 1)
    print(rank, data)
    data = comm.recv(source = 0, tag = 0)
    print(rank, data)




if rank == 0:
    data = comm.recv(source = 1, tag = 1)
    print(rank, data)

    data = "master2slave"
    comm.send(data, dest = 1, tag = 0)
    print(rank, data)

else:
    data = comm.recv(source = 0, tag = 1)
    print(rank, data)

    data = "slave2master"
    comm.send(data, dest = 0, tag = 0)
    print(rank, data)




if rank == 0:
    data = np.empty(10, dtype = int)
    comm.Irecv([data, MPI.INT], source = 1, tag = 1)
    print(rank, data)

    data = np.arange(10, dtype = int)
    comm.Isend([data, MPI.INT], dest = 1, tag = 0)
    print(rank, data)

else:
    data = np.empty(10, dtype = int)
    comm.Irecv([data, MPI.INT], source = 0, tag = 1)
    print(rank, data)

    data = np.arange(10, dtype = int)
    comm.Isend([data, MPI.INT], dest = 0, tag = 0)
    print(rank, data)




if rank == 0:
    for i in range(10):
        comm.send(i, dest = 1, tag = 0)
    print("Master has done sending")
else:
    while True:
        data_probe = False
        #data_probe = comm.probe(source = 0, tag = 0)
        data_probe = comm.iprobe(source = 0, tag = 0)
        if data_probe:
            data = comm.recv(source = 0, tag = 0)
            print(f"Slave received {data}")
            if data == 9:
                break
        print("Waiting")
    print("Slave has received all")
