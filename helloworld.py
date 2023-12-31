from mpi4py import MPI

def parse_input_file(filename):
    with open(filename, 'r') as file:
        num_machines = int(file.readline().strip())
        num_cycles = int(file.readline().strip())
        wear_factors = list(map(int, file.readline().strip().split()))
        maintenance_threshold = int(file.readline().strip())

        tree_structure = {}
        for _ in range(num_machines - 1):
            child, parent, operation = file.readline().strip().split()
            tree_structure[int(child)] = {'parent': int(parent), 'operation': operation}

        leaf_products = {}
        for _ in range(num_machines - len(tree_structure)):
            line = file.readline().strip()
            print("Reading line for leaf machine:", line)  # Debug print
            machine_id, product = line.split()
            leaf_products[int(machine_id)] = product

    return {
        'num_machines': num_machines,
        'num_cycles': num_cycles,
        'wear_factors': wear_factors,
        'maintenance_threshold': maintenance_threshold,
        'tree_structure': tree_structure,
        'leaf_products': leaf_products
    }

def main():
    # Initialize MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    
    # Control room (master process)
    if rank == 0:
        # Parse the input file and distribute information (to be implemented)
        input_file_path = "input.txt"
        input_data = parse_input_file(input_file_path)
        print(input_data)
        # Start the factory simulation (to be implemented)
        # Wait for the factory simulation to finish (to be implemented)
        # Print the final results (to be implemented)
        print("Control Room: Initializing factory simulation")
        # Parse the input file and distribute information (to be implemented)
    else:
        print(f"Machine {rank}: Ready for operation")

if __name__ == "__main__":
    main()

