from mpi4py import MPI

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    print(f"Hello from rank {rank}")

    import os
    print(os.environ)



if __name__ == "__main__":
    main()
