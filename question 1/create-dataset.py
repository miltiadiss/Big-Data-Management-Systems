import random
import os
from uxsim import World

seed = None

W = World(
    name="",
    deltan=5,
    tmax=3600,
    print_mode=1, save_mode=0, show_mode=1,
    random_seed=seed,
    duo_update_time=600
)

random.seed(seed)

# Ορισμός δικτύου
"""
    N1  N2  N3  N4  N5  N6
    |   |   |   |   |   |
W1--I1--I2--I3--I4--I5--I6--E1
    |   |   |   |   |   |
    v   ^   v   ^   v   ^
    S1  S2  S3  S4  S5  S6
"""

signal_time = 20
sf_1 = 1
sf_2 = 1

# Δημιουργία κόμβων
nodes = {
    'I1': W.addNode("I1", 1, 0, signal=[signal_time * sf_1, signal_time * sf_2]),
    'I2': W.addNode("I2", 2, 0, signal=[signal_time * sf_1, signal_time * sf_2]),
    'I3': W.addNode("I3", 3, 0, signal=[signal_time * sf_1, signal_time * sf_2]),
    'I4': W.addNode("I4", 4, 0, signal=[signal_time * sf_1, signal_time * sf_2]),
    'I5': W.addNode("I5", 5, 0, signal=[signal_time * sf_1, signal_time * sf_2]),
    'I6': W.addNode("I6", 6, 0, signal=[signal_time * sf_1, signal_time * sf_2]),
    'W1': W.addNode("W1", 0, 0),
    'E1': W.addNode("E1", 7, 0),
    'N1': W.addNode("N1", 1, 1),
    'N2': W.addNode("N2", 2, 1),
    'N3': W.addNode("N3", 3, 1),
    'N4': W.addNode("N4", 4, 1),
    'N5': W.addNode("N5", 5, 1),
    'N6': W.addNode("N6", 6, 1),
    'S1': W.addNode("S1", 1, -1),
    'S2': W.addNode("S2", 2, -1),
    'S3': W.addNode("S3", 3, -1),
    'S4': W.addNode("S4", 4, -1),
    'S5': W.addNode("S5", 5, -1),
    'S6': W.addNode("S6", 6, -1),
}

# Δημιουργία συνδέσμων
# Ε <-> W κατεύθυνση: signal group 0
links = [
    ('W1', 'I1'), ('I1', 'I2'), ('I2', 'I3'), ('I3', 'I4'), ('I4', 'I5'), ('I5', 'I6'), ('I6', 'E1')
]

for n1, n2 in links:
    W.addLink(f"{n2}{n1}", nodes[n2], nodes[n1], length=500, free_flow_speed=50, jam_density=0.2, number_of_lanes=3, signal_group=0)

# N -> S κατεύθυνση: signal group 1
links_ns = [
    ('N1', 'I1'), ('I1', 'S1'), ('N2', 'I2'), ('I2', 'S2'), ('N3', 'I3'), ('I3', 'S3'),
    ('N4', 'I4'), ('I4', 'S4'), ('N5', 'I5'), ('I5', 'S5'), ('N6', 'I6'), ('I6', 'S6')
]

for n1, n2 in links_ns:
    W.addLink(f"{n1}{n2}", nodes[n1], nodes[n2], length=500, free_flow_speed=30, jam_density=0.2, signal_group=1)

# S -> N κατεύθυνση: signal group 2
for n1, n2 in links_ns:
    W.addLink(f"{n2}{n1}", nodes[n2], nodes[n1], length=500, free_flow_speed=30, jam_density=0.2, signal_group=2)

# Ορισμός τυχαίων απαιτήσεων κάθε 30 δευτερόλεπτα
dt = 30
demand = 2  # μέση ζήτηση για το χρόνο εξομοίωσης
demands = []
for t in range(0, 3600, dt):
    dem = random.uniform(0, demand)
    for n1, n2 in [['N1', 'S1'], ['N2', 'S2'], ['N3', 'S3'], ['N4', 'S4'], ['N5', 'S5'], ['N6', 'S6']]:
        W.adddemand(nodes[n1], nodes[n2], t, t + dt, dem * 0.25)
        demands.append({"start": n1, "dest": n2, "times": {"start": t, "end": t + dt}, "demand": dem})
    for n1, n2 in [['E1', 'W1'], ['N1', 'W1'], ['S2', 'W1'], ['N3', 'W1'], ['S4', 'W1'], ['N5', 'W1'], ['S6', 'W1']]:
        W.adddemand(nodes[n1], nodes[n2], t, t + dt, dem * 0.75)
        demands.append({"start": n1, "dest": n2, "times": {"start": t, "end": t + dt}, "demand": dem})

W.exec_simulation()

subfolder = 'sources'
if not os.path.exists(subfolder):
    os.makedirs(subfolder)
csv_file_path = os.path.join(subfolder, 'vehicles.csv')

W.analyzer.vehicles_to_pandas().to_csv(csv_file_path, index=False)
