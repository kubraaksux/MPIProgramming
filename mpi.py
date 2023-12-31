from mpi4py import MPI
import sys
import numpy as np
from mpi4py.futures import MPIPoolExecutor

# Initialize MPI environment
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank= comm.Get_rank()

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
    return ''.join(products)

def enhance_operation(product):
    if len(product) == 0:
        return product
    return product[0] + product + product[-1]

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

# Parse the input file and extract initial settings
def parse_input_file(input_file):
    global size

    with open(input_file, 'r') as f:
        lines = f.readlines()

    num_machines = int(lines[0].strip())
    size = num_machines
    num_cycles = int(lines[1].strip())
    wear_factors_values = list(map(int, lines[2].strip().split()))
    threshold = int(lines[3].strip())

    # Initialize the wear factors dictionary for operations
    wear_factors = {op: wear_factors_values[i-1] for i, op in enumerate(operations) if i!=0}

    # Initialize dictionaries for parent-child relations and machine operations
    machines = {}

    # Parsing the machine operations and parent-child relations
    for line in lines[4:4 + num_machines -1]:
        machine_id, parent_id, operation = line.strip().split()
        print(machine_id, " ", parent_id)
        machine_id, parent_id = int(machine_id), int(parent_id)
        if machine_id not in machines.keys():
            machines[machine_id]={}
            machines[machine_id]["children"]=[]
            machines[machine_id]["operation"]=operation
            machines[machine_id]["initialProduct"]=None
            machines[machine_id]["parent"] = None
        else:
            machines[machine_id]["operation"]=operation
        
        if parent_id not in machines.keys():
            machines[parent_id]={}
            machines[parent_id]["children"]=[]
            machines[parent_id]["children"].append(machine_id)
            machines[machine_id]["parent"] = parent_id
            machines[parent_id]["parent"] = None
            machines[parent_id]["operation"]='add'
            machines[parent_id]["initialProduct"]=None
        else:
            machines[parent_id]["children"].append(machine_id)
            machines[machine_id]["parent"] = parent_id

    # Parsing initial products for leaf machines

    leafIDs=[key for key in machines.keys() if not machines[key]["children"]]
    leafIDs.sort()
    for index,line in enumerate(lines[4 + num_machines:]):
        initialProduct = line.strip()
        machines[leafIDs[index]]["initialProduct"] = initialProduct

    return num_cycles, threshold, wear_factors, dict((sorted(machines.items())))

def test_parse_input_file():
    expected_output = {
        'num_machines': 7,
        'num_cycles': 10,
        'wear_factors': {...},  # fill in expected wear factors
        'machines': {...},      # fill in expected machine details
    }

    actual_output = parse_input_file('input.txt')
    assert actual_output == expected_output, "Parsed output does not match expected output"

test_parse_input_file()


# Calculate the maintenance cost and send the maintenance log to the control room   
def calculate_maintenance_cost(accumulated_wear, threshold, wear_factor, cycle):
    # maintenance_cost = 0
    
    # if accumulated_wear >= threshold:
    #     maintenance_cost = (accumulated_wear - threshold + 1) * wear_factor
    #     maintenance_message = f"{rank}-{maintenance_cost}-{cycle}"

    #     # Use MPI.Request for non-blocking send
    #     request = comm.isend(maintenance_message, dest=0, tag=maintenance_tag) # Non-blocking send
    #     request.wait()
        
    return (accumulated_wear - threshold + 1) * wear_factor


# Worker process logic for simulating factory machines
def machine_process(args):

    rank = args[0]
    
    parent=args[1]
    children=args[2]
    initial_operation=args[3]
    wear_factors=args[4]
    threshold=args[5]
    num_cycles=args[6]
    initial_product=args[7]

    accumulated_wear = 0
    global operation_sequence 

    operation_sequence = operation_sequence['odd' if rank % 2 else 'even']
    if initial_operation != 'add':
        current_operation_index = operation_sequence.index(initial_operation)

    for cycle in range(num_cycles):
        # Receive products from children or use initial product if it is a leaf machine
        received_products = {}
        processed_product = ""
        if children:
            for child in children:
                
                product = bytearray(product_shape)
                print("rank: ",rank, " ", "cycle: ", cycle, " ","child: ", child)                
                comm.Recv([product,product_dtype], source=child)
                #received_products[child]=comm.recv(source=child)
                received_products[child]=product.decode('utf-8')
                
        else:
            processed_product = initial_product

        # Sort the received_products and send it to add operation
        if not received_products:
            received_products=dict(sorted(received_products.items()))
            processed_product = operations['add'](list(received_products.values()))
        # the control for the terminal machine--perform the current operation except it is the terminal machine
        if initial_operation != 'add':
            processed_product = operations[initial_operation](processed_product)
            accumulated_wear += wear_factors[initial_operation]

        

        # if it's the terminal machine send data to parent or control room.
        if parent is not None:
            print("rank: ",rank, " ", "cycle: ", cycle, " ","parent: ", parent)
            # Use blocking send to ensure parent takes the product
            comm.Send([processed_product.encode('utf-8'),product_dtype],dest=parent)
            #comm.Send([processed_product.encode('utf-8'),product_dtype],dest=parent)
        else:
            # For the terminal machine, blocking send the final product to the control room
            print(parent)
            comm.Send([processed_product.encode('utf-8'),product_dtype],dest=99)

        # Check for maintenance
        if accumulated_wear >= threshold:
            maintenance_cost = calculate_maintenance_cost(accumulated_wear, threshold, wear_factors[initial_operation])
            maintenance_message = f"{rank}-{maintenance_cost}-{cycle}"
            # non-blocking send for wear messages
            comm.isend(maintenance_message, dest=0, tag=maintenance_tag)  # Non-blocking send
            accumulated_wear = 0  # Reset wear after maintenance

        # Update current operation for the next cycle
        current_operation_index = (current_operation_index + 1) % len(operation_sequence)
        initial_operation = operation_sequence[current_operation_index]

    


# Control room logic for distributing initial data and collecting final product and maintenance logs
def main_control_room(input_file, output_file):
    # Parse the input file to get initial settings
    num_cycles, threshold, wear_factors, machines = parse_input_file(input_file)
    global size
    print(machines)
    # Initialize a dictionary to store the data to be sent to each machine
    machine_data = []


    # Distribute initial data to each machine
    print(size)
    for machine_id in range(1,size+1):
        machine_data.append([machine_id, machines[machine_id]["parent"], machines[machine_id]["children"], machines[machine_id]["operation"], wear_factors, threshold, num_cycles, machines[machine_id]["initialProduct"]])

    print(machine_data)
    # Use MPIPoolExecutor for distributing initial data
    with MPIPoolExecutor (max_workers=size + 1) as executor:        
        # Prepare and distribute data to each machine
        futures = list(executor.map(machine_process, machine_data))
        # Ensure all data is sent
        for future in futures:
            future.result()

    # Collect final product and maintenance logs
    final_products = []
    maintenance_logs = []

    # Receive final product and maintenance logs from each machine
    for _ in range(size):
        status = MPI.Status()
        data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        if status.tag == 99:
            final_products.append(data)
        else:
            maintenance_logs.append(data)

    # Handle any remaining incoming maintenance messages
    while comm.Iprobe(source=MPI.ANY_SOURCE, tag=maintenance_tag):
        maintenance_log = np.empty(log_size, dtype='S')
        request = comm.irecv(maintenance_log, source=MPI.ANY_SOURCE, tag=maintenance_tag)
        request.Wait() # Wait for the non-blocking receive to complete
        print(f"Machine {rank} received maintenance log: {maintenance_log.tostring().decode('utf-8')}")
        maintenance_logs.append(maintenance_log)

    # Write the final product and maintenance logs to the output file
    with open(output_file, 'w') as f:
        for final_product in final_products:
            f.write(f"Master Received: {final_product}\n")
        for log in maintenance_logs:
            f.write(f"{log}\n")

if __name__ == "__main__":
    if rank == 0:
        if len(sys.argv) != 3:
            print("Usage: mpiexec -n 1 python mpi.py input.txt output.txt")
            sys.exit(1)
        # Control room logic
        input_file, output_file = sys.argv[1:3]
        main_control_room(input_file, output_file)
    else:
        # Machine logic
        # Receive initial settings (blocking receive)
        parent, children, initial_operation, received_wear_factors, received_threshold, num_cycles, initial_product = comm.recv(source=0)
        machine_process(rank, parent, children, initial_operation, received_wear_factors, received_threshold, num_cycles, initial_product)

