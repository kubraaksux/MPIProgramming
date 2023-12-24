from mpi4py import MPI
from mpi4py.futures import MPIPoolExecutor
import sys
import numpy as np

# Initialize MPI environment
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
status = MPI.Status()

product_shape = (150,)  # Maximum length of product strings after processing
product_dtype = 'S150'  # String type with a maximum length of 150 characters
log_size = 30           # Maximum length of maintenance log entries
maintenance_tag = 77   # Unique tag for maintenance messages
operations = ['enhance', 'reverse', 'chop', 'trim', 'split']
wear_factors = {}

'''# Operations dictionary to hold wear factor for each operation
wear_factors = {'add': 0, 'enhance': 2, 'reverse': 2, 'chop': 1, 'trim': 1, 'split': 1}
operation_sequence = {
    'odd': ['reverse', 'trim'],
    'even': ['enhance', 'split', 'chop']
}'''

# Function to parse the input file and initialize machine settings
def parse_input_file(input_file):
    with open(input_file, 'r') as f:
        lines = f.readlines()

    num_machines = int(lines[0].strip())
    num_cycles = int(lines[1].strip())
    wear_factors_values = list(map(int, lines[2].strip().split()))
    for i, operation in enumerate(operations):
        wear_factors[operation] = wear_factors_values[i]
    threshold = int(lines[3].strip())
    adjacency_list = [list(map(int, line.strip().split())) for line in lines[4:4 + num_machines - 1]]
    initial_products = lines[4 + num_machines - 1:]

    # Construct the parent-child relationship and initial operation mapping
    machine_operations = {}
    parent_child_relations = {i: [] for i in range(1, num_machines + 1)}
    for child_id, parent_id, *operation in adjacency_list:
        parent_child_relations[parent_id].append(child_id)
        machine_operations[child_id] = operation[0] if operation else 'add'

    # Map initial products to leaf machines based on ID
    leaf_machines = sorted(set(range(1, num_machines + 1)) - set(machine_operations.keys()))
    initial_products_mapping = {leaf_machines[i]: prod.strip() for i, prod in enumerate(initial_products)}

    return num_cycles, threshold, machine_operations, parent_child_relations, initial_products_mapping

# Define operation functions for factory machines
def add_operation(products):
    # The function logic should be updated according to the actual operation
    return ''.join(sorted(products, key=lambda x: int(x.split('-')[1])))

def enhance_operation(product):
    return product[0] + product + product[-1] if len(product) > 0 else product

def reverse_operation(product):
    return product[::-1]

def chop_operation(product):
    return product[:-1] if len(product) > 1 else product

def trim_operation(product):
    return product[1:-1] if len(product) > 2 else product

def split_operation(product):
    mid = len(product) // 2
    return product[:mid] if len(product) % 2 == 0 else product[:mid + 1]

# Function to be executed on worker nodes, operation functions for factory machines
def process_data(product):
    # Data processing logic
    pass

def calculate_maintenance_cost(wear, threshold, wear_factor):
    if wear >= threshold:
        return (wear - threshold + 1) * wear_factor
    return 0

# Worker process logic for simulating factory machines
def machine_process(rank, parent, children, operations, wear_factors, threshold, num_cycles, initial_products):
    current_operation_index = 0
    accumulated_wear = 0
    cycle = 0
    is_terminal_machine = parent is None
    # Determine the sequence of operations for this machine based on its ID
    operations = operation_sequence['odd' if rank % 2 else 'even']
    current_operation = operations[current_operation_index]

    # The initial operation for each machine is set by the control room
    # This information should be received by the worker process
    # For example, initial_operation = "trim" or "split" etc.

    while True:
        # Receive products from child machines (blocking)
        received_products = []
        if not is_terminal_machine:
            for child in children:
                product = np.empty(product_shape, dtype=product_dtype)
                comm.Recv(product, source=child)
                received_products.append(product.tostring().decode('utf-8'))

        # Terminal machine starts with initial products
        else:
            received_products = initial_products

        # Add operation is always performed
        processed_product = add_operation(received_products)

        # Perform operations
        processed_product = add_operation(received_products)
        if not is_terminal_machine:
            operation_function = globals()[f"{current_operation}_operation"]
            processed_product = operation_function(processed_product)
            accumulated_wear += wear_factors[current_operation]  # Add wears out for non-terminal machines

        '''# Process received products with each operation
        for operation in operations:
            products = [operation(product) for product in products]'''

        # Send processed product to parent or inform control room if terminal machine
        if not is_terminal_machine:
            comm.Send(np.array(processed_product, dtype=product_dtype), dest=parent)
        else:
            # Terminal machine sends final product to control room
            comm.Send(np.array(processed_product, dtype=product_dtype), dest=0, tag=99)
            break  # Final product produced, terminate loop

        # Maintenance check and message sending
        if accumulated_wear >= threshold:
            maintenance_cost = calculate_maintenance_cost(accumulated_wear, threshold, wear_factors[current_operation])
            maintenance_message = f"{rank}-{maintenance_cost}-{cycle}"
            # Non-blocking send of the maintenance message to the control room
            req = comm.isend(maintenance_message, dest=0, tag=maintenance_tag)
            req.wait()  # Ensure the message is sent before continuing
            accumulated_wear = 0  # Reset wear after maintenance

        # Update current operation for the next cycle
        current_operation_index = (current_operation_index + 1) % len(operations)
        current_operation = operations[current_operation_index]
        cycle += 1

        # Check for incoming maintenance messages before starting operations
        while comm.Iprobe(source=MPI.ANY_SOURCE, tag=maintenance_tag):
            maintenance_log = np.empty(log_size, dtype='S')
            comm.Recv(maintenance_log, source=MPI.ANY_SOURCE, tag=maintenance_tag)
            print(f"Machine {rank} received maintenance log: {maintenance_log.tostring().decode('utf-8')}")

        # Update current operation for the next cycle
        current_operation_index = (current_operation_index + 1) % len(operations)
        current_operation = operations[current_operation_index]
        cycle += 1

        '''# Terminal machine logic
        if not children:
            final_product = process_product(products)  # Assuming process_product is defined
            comm.Send([np.array(final_product, dtype='S'), MPI.CHAR], dest=0, tag=99)  # Blocking send
            break  # Exit loop as job is done

        # Non-terminal machine logic
        else:
            processed_product = process_product(products)  # Assuming process_product is defined
            comm.Send([np.array(processed_product, dtype='S'), MPI.CHAR], dest=parent)  # Blocking send '''


# Main control room logic for initializing and orchestrating the simulation
def main_control_room(input_file, output_file, size):
    num_cycles, threshold, machine_operations, parent_child_relations, initial_products_mapping = parse_input_file(input_file)
    operations = ['example_operation']
    wear_factors = {'example_operation': 1}
    threshold = 13

    # Assuming parent_child_relationships is predefined
    # This should be a dictionary mapping machine_id to its (parent, children) tuple
    parent_child_relationships = {}  # Initialize or load from configuration

        # Distribute initial configuration to each machine
    for machine_id in range(1, size):
        parent, children = parent_child_relationships.get(machine_id, (None, []))
        comm.send((parent, children, operations, wear_factors, threshold), dest=machine_id)

    # Collect final product and maintenance logs
        final_product = None
        maintenance_logs = []
        for _ in range(size - 1):
            status = MPI.Status()
            data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            if status.tag == 99:
                final_product = data
            else:
                maintenance_logs.append(data)
    
    # Output final product and maintenance logs
    with open(output_file, 'w') as f:
        f.write(f"Final product: {final_product}\n")
        for log in maintenance_logs:
            f.write(f"{log}\n")

    # Master process logic for initial input distribution
    with open(input_file, 'r') as f:
        lines = f.readlines()
    data_to_process = [line.strip() for line in lines]

    # Using MPIPoolExecutor to distribute and process data
    with MPIPoolExecutor() as executor:
        results = executor.map(process_data, data_to_process)
        for result in results:
            print(f"Processed data: {result}")


if __name__ == "__main__":
    if rank == 0:
        if len(sys.argv) != 3:
            print("Usage: mpiexec -n 1 python mpi.py input.txt output.txt")
            sys.exit(1)

        input_file, output_file = sys.argv[1:3]
        main_control_room(input_file, output_file)
    else:
        # Worker process logic
        # Initialize worker processes based on the project requirements
        # For simplicity, assuming these variables are received from the master process
        parent, children, operations, wear_factors, threshold = comm.recv(source=0)
        machine_process(rank, parent, children, operations, wear_factors, threshold)
    '''which one??'''
        parent, children, initial_operation, threshold, num_cycles, initial_product = comm.recv(source=0)
        machine_process(rank, parent, children, initial_operation, threshold, num_cycles, initial_product)

# Finalize MPI
MPI.Finalize()
