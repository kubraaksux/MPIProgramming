from mpi4py import MPI
from concurrent.futures import ProcessPoolExecutor
import sys
import numpy as np
from mpi4py.futures import MPIPoolExecutor

# Initialize MPI environment
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
status = MPI.Status()

# Define data types and constants
product_shape = (150,)  # Maximum length of product strings after processing
product_dtype = MPI.CHAR
log_size = 30           # Maximum length of maintenance log entries
maintenance_tag = 77   # Unique tag for maintenance messages
threshold = None 
wear_factors = {}

# Operations
def add_operation(products):
    """
    Concatenate a list of product strings.
    
    Args:
        products (list): List of product strings.
    
    Returns:
        str: Concatenated product string.
    """
    return ''.join(products)

def enhance_operation(product):
    """
    Enhance a product string by adding its first and last characters.
    
    Args:
        product (str): Input product string.
    
    Returns:
        str: Enhanced product string.
    """
    if len(product) == 0:
        return product
    return product[0] + product + product[-1]

def reverse_operation(product):
    """
    Reverse a product string.
    
    Args:
        product (str): Input product string.
        
    Returns:
        str: Reversed product string.
    """
    return product[::-1]

def chop_operation(product):
    """
    Chop off the first and last characters of a product string.

    Args:
        product (str): Input product string.

    Returns:
        str: Chopped product string.
    """
    return product[:-1] if len(product) > 1 else product

def trim_operation(product):
    """
    Trim off the first and last characters of a product string.
    
    Args:
        product (str): Input product string.

    Returns:
        str: Trimmed product string.
    """
    return product[1:-1] if len(product) > 2 else product

def split_operation(product):
    """
    Split a product string into two halves.

    Args:
        product (str): Input product string.
    
    Returns:
        str: First half of the product string if the length is even, or the first half plus the middle character if the length is odd.
    """
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



# Calculate the maintenance cost and send the maintenance log to the control room   
def calculate_maintenance_cost(accumulated_wear, threshold, wear_factor, cycle):
    maintenance_cost = 0
    
    if accumulated_wear >= threshold:
        maintenance_cost = (accumulated_wear - threshold + 1) * wear_factor
        maintenance_message = f"{rank}-{maintenance_cost}-{cycle}"

        # Use MPI.Request for non-blocking send
        request = comm.isend(maintenance_message, dest=0, tag=maintenance_tag) # Non-blocking send
        request.wait()
        
    return maintenance_cost


# Worker process logic for simulating factory machines
def machine_process(rank, parent, children, initial_operation, wear_factors, threshold, num_cycles, initial_products):
    accumulated_wear = 0
    global operation_sequence 
    operation_sequence = operation_sequence['odd' if rank % 2 else 'even']
    current_operation_index = operation_sequence.index(initial_operation)

    for cycle in range(num_cycles):
        # Receive products from children or use initial product if a leaf machine
        received_products = []
        if children:
            for child in children:
                product = np.empty(product_shape, dtype=product_dtype)
                comm.Recv(product, source=child)
                received_products.append(product.tostring().decode('utf-8'))
        else:
            received_products = [initial_products[rank]]

        # Perform add operation
        processed_product = operations['add'](received_products)

        # Perform the current operation if it's not the add operation (for non-terminal machines)
        if initial_operation != 'add':
            processed_product = operations[initial_operation](processed_product)
            accumulated_wear += wear_factors[initial_operation]

        # Send processed product to the parent or inform control room if it's the terminal machine
        if parent is not None:
            # Use blocking `Send` to ensure parent takes the product
            comm.Send(np.array(processed_product, dtype=product_dtype), dest=parent)
        else:
            # For the terminal machine, send the final product to the control room
            comm.Send(np.array(processed_product, dtype=product_dtype), dest=0, tag=99)

        # Check for maintenance
        if accumulated_wear >= threshold:
            maintenance_cost = calculate_maintenance_cost(accumulated_wear, threshold, wear_factors[initial_operation])
            maintenance_message = f"{rank}-{maintenance_cost}-{cycle}"
            # Use non-blocking `Isend` for wear messages
            comm.isend(maintenance_message, dest=0, tag=maintenance_tag)  # Non-blocking send
            accumulated_wear = 0  # Reset wear after maintenance

        # Update current operation for the next cycle
        current_operation_index = (current_operation_index + 1) % len(operation_sequence)
        initial_operation = operation_sequence[current_operation_index]

    # Handle any remaining incoming maintenance messages
    while comm.Iprobe(source=MPI.ANY_SOURCE, tag=maintenance_tag):
        maintenance_log = np.empty(log_size, dtype='S')
        request = comm.irecv(maintenance_log, source=MPI.ANY_SOURCE, tag=maintenance_tag)
        request.Wait() # Wait for the non-blocking receive to complete
        print(f"Machine {rank} received maintenance log: {maintenance_log.tostring().decode('utf-8')}")


# Control room logic for distributing initial data and collecting final product and maintenance logs
def main_control_room(input_file, output_file, size):
    # Parse the input file to get initial settings
    num_cycles, threshold, wear_factors, parent_child_relations, machine_operations, initial_products = parse_input_file(input_file)

    # Initialize a dictionary to store the data to be sent to each machine
    machine_data = {}

    # Distribute initial data to each machine
    for machine_id in range(1, size):
        parent, children = parent_child_relations.get(machine_id, (None, []))
        initial_operation = machine_operations.get(machine_id, 'add')  # Default to 'add' if operation is not specified
        initial_product = initial_products.get(machine_id, None)
        machine_data[machine_id] = (parent, children, initial_operation, wear_factors, threshold, num_cycles, initial_product)

    # Use MPIPoolExecutor for distributing initial data
    with MPIPoolExecutor(max_workers=size) as executor:
        # Prepare and distribute data to each machine
        futures = []
        for machine_id, data_to_send in machine_data.items():
            future = executor.submit(comm.send, data_to_send, dest=machine_id)
            futures.append(future)

        # Ensure all data is sent
        for future in futures:
            future.result()

    # Collect final product and maintenance logs
    final_product = None
    maintenance_logs = []

    # Receive final product and maintenance logs from each machine
    for _ in range(size - 1):
        status = MPI.Status()
        data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        if status.tag == 99:
            final_product = data
        else:
            maintenance_logs.append(data)

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

