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
product_dtype = MPI.CHAR
log_size = 30           # Maximum length of maintenance log entries
maintenance_tag = 77   # Unique tag for maintenance messages
threshold = None 
wear_factors = {}
operations = {}

# Operations
def add_operation(products):
    return ''.join(products)

def enhance_operation(product):
    return product[0] + product + product[-1] if product else product

def reverse_operation(product):
    return product[::-1]

def chop_operation(product):
    return product[:-1] if len(product) > 1 else product

def trim_operation(product):
    return product[1:-1] if len(product) > 2 else product

def split_operation(product):
    mid = len(product) // 2
    return product[:mid] if len(product) % 2 == 0 else product[:mid + 1]

# Operation Mapping and Wear Factors
operations = {
    'add': add_operation,
    'enhance': enhance_operation,
    'reverse': reverse_operation,
    'chop': chop_operation,
    'trim': trim_operation,
    'split': split_operation
}

# Alternation of operations based on machine ID
operation_sequence = {
    'odd': ['reverse', 'trim'],  # Odd machines alternate between reverse and trim
    'even': ['enhance', 'split', 'chop']  # Even machines alternate between enhance, split, and chop
}

def parse_input_file(input_file):
    with open(input_file, 'r') as f:
        lines = f.readlines()

    num_machines = int(lines[0].strip())
    num_cycles = int(lines[1].strip())
    wear_factors_values = list(map(int, lines[2].strip().split()))

    wear_factors_line = lines[3].split()
    wear_factors = {
        'enhance': int(wear_factors_line[0]),
        'reverse': int(wear_factors_line[1]),
        'chop': int(wear_factors_line[2]),
        'trim': int(wear_factors_line[3]),
        'split': int(wear_factors_line[4]),
    }
    threshold = int(lines[4])
    wear_factors = {op: wear_factors_values[i] for i, op in enumerate(operations) if op != 'add'}
    threshold = int(lines[3].strip())

    parent_child_relations = {i: [] for i in range(1, num_machines + 1)}
    machine_operations = {}
    for line in lines[4:4 + num_machines - 1]:
        child_id, parent_id, *operation = map(int, line.strip().split())
        parent_child_relations[parent_id].append(child_id)
        machine_operations[child_id] = operation[0] if operation else 'add'

    initial_products = {}
    for line in lines[4 + num_machines - 1:]:
        machine_id, product = line.strip().split(':')
        initial_products[int(machine_id)] = product

    return num_cycles, threshold, wear_factors, parent_child_relations, machine_operations, initial_products


# Function to be executed on worker nodes, operation functions for factory machines
def process_data(product):
    # Data processing logic
    pass


def calculate_maintenance_cost(accumulated_wear, threshold, wear_factor):
    if accumulated_wear >= threshold:
        return (accumulated_wear - threshold + 1) * wear_factor
    return 0

# Worker process logic for simulating factory machines
def machine_process(rank, parent, children, operations, wear_factors, threshold, num_cycles, initial_products):
    current_operation = initial_operation
    current_operation_index = 0
    accumulated_wear = 0
    current_operation = 'add'  # Default initial operation
    cycle = 0
    operation_sequence = ['reverse', 'trim'] if rank % 2 else ['enhance', 'split', 'chop']

    is_terminal_machine = parent is None
    # Determine the sequence of operations for this machine based on its ID
    operations = operation_sequence['odd' if rank % 2 else 'even']
    current_operation = operations[current_operation_index]

    # The initial operation for each machine is set by the control room
    # This information should be received by the worker process
    # For example, initial_operation = "trim" or "split" etc.

    for cycle in range(num_cycles):
        # Receive products from children (if not a leaf machine)
        received_products = []
        if children:
            for child in children:
                product = np.empty(product_shape, dtype=product_dtype)
                comm.Recv(product, source=child)
                received_products.append(product.tostring().decode('utf-8'))
        else:
            received_products = [initial_product]

        # Perform operations on the received products
        processed_product = operations['add'](received_products)
        if current_operation != 'add':
            processed_product = operations[current_operation](processed_product)
            accumulated_wear += wear_factors[current_operation]

        # Perform operations
        processed_product = add_operation(received_products)
        if not is_terminal_machine:
            operation_function = globals()[f"{current_operation}_operation"]
            processed_product = operation_function(processed_product)
            accumulated_wear += wear_factors[current_operation]  # Add wears out for non-terminal machines

        # Send processed product to the parent or inform control room if it's the terminal machine
        if parent is not None:
            comm.Send(np.array(processed_product, dtype=product_dtype), dest=parent)
        else:
            comm.Send(np.array(processed_product, dtype=product_dtype), dest=0, tag=99)

        # Check for maintenance
        if accumulated_wear >= threshold:
            maintenance_cost = calculate_maintenance_cost(accumulated_wear, threshold, wear_factors[current_operation])
            maintenance_message = f"{rank}-{maintenance_cost}-{cycle}"
            comm.isend(maintenance_message, dest=0, tag=maintenance_tag)  # Non-blocking send
            accumulated_wear = 0  # Reset wear after maintenance

        # Update current operation for the next cycle
        current_operation = operation_sequence[(cycle + 1) % len(operation_sequence)]
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


# Main control room logic for initializing and orchestrating the simulation
def main_control_room(input_file, output_file, size):
    # Parse the input file to get initial settings
    num_cycles, threshold, wear_factors, parent_child_relations, machine_operations, initial_products = parse_input_file(input_file)

    # Use MPIPoolExecutor for distributing initial data
    with MPIPoolExecutor(max_workers=size) as executor:
        # Prepare and distribute data to each machine
        futures = []
        for machine_id in range(1, size):
            parent = parent_child_relations[machine_id]['parent']
            children = parent_child_relations[machine_id].get('children', [])
            initial_operation = machine_operations[machine_id]
            initial_product = initial_products.get(machine_id, None)
            data_to_send = (machine_id, parent, children, initial_operation, wear_factors, threshold, num_cycles, initial_product)
            future = executor.submit(comm.send, data_to_send, dest=machine_id)
            futures.append(future)
        
        # Ensure all data is sent
        for future in futures:
            future.result()

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

    '''
    # Master process logic for initial input distribution
    with open(input_file, 'r') as f:
        lines = f.readlines()
        data_to_process = [line.strip() for line in lines]'''

    # Write the final product and maintenance logs to the output file
    with open(output_file, 'w') as f:
        if final_product:
            f.write(f"Final product: {final_product}\n")
        for log in maintenance_logs:
            f.write(f"{log}\n")


if __name__ == "__main__":
    if rank == 0:
        if len(sys.argv) != 3:
            print("Usage: mpiexec -n 1 python mpi.py input.txt output.txt")
            sys.exit(1)
        # Control room logic
        input_file, output_file = sys.argv[1:3]
        main_control_room(input_file, output_file, size)
    else:
        # Machine logic
        # Receive initial settings (blocking receive)
        parent, children, initial_operation, received_wear_factors, received_threshold, num_cycles, initial_product = comm.recv(source=0)
        machine_process(rank, parent, children, operations, received_wear_factors, received_threshold, num_cycles, initial_product)
        initial_data = comm.recv(source=0)
        machine_process(*initial_data)

# Finalize MPI
MPI.Finalize()
