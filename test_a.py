from itertools import product as p
eq_id = [613, 612, 616, 614, 614, 615, 616, 617, 618, 619, 614, 616, 615, 617, 689, 7, 8, 9, 615, 617, 620, 621]
eq_type = ['0. Bus', '0. Bus', '1. Transformer HT', '1. Transformer HT', '2. Transformer', '2. Transformer',
           '2. Transformer', '2. Transformer', '4. Line', '4. Line', '7. Breaker', '7. Breaker', '3. Transformer LT',
           '3. Transformer LT', '5. Feeder', '6. Bank', '6. Bank', '6. Bank', '7. Breaker', '7. Breaker', '7. Breaker',
           '7. Breaker']
voltage_level = ['132', '132', '132', '132', '132', '132', '132', '132', '132', '132', '132', '132', '33', '33', '33',
                 '33', '33', '33', '33', '33', '33', '33']
eq_name = ['Bus-2', 'Bus-1', 'GT-3 HT', 'GT-2 HT', 'GT-2', 'GT-2', 'GT-3', 'GT-3', 'Manikganj- Kabirpur CKT-1',
           'Manikganj- Kabirpur CKT-2', 'GT-2 Transformer', 'GT-3 Transformer', 'GT-2 LT', 'GT-3 LT',
           'Spectra Solar Park', 'Cap. Bank -1', 'Cap. Bank -2', 'Inductor Bank-1', 'GT-2 Incomer ', 'GT-3 Incomer',
           'Cap. Bank-1', 'Cap. Bank-2']
tuples = list(zip(voltage_level, eq_type, eq_name, eq_id))

idx = []
p_q_i = ['mega_watt', 'mega_var', 'Ampere R', 'Ampere Y', 'Ampere B']
reading_dict = {
    '0. Bus': ['voltage'],  # Frequency could be added
    '1. Transformer HT': p_q_i,
    '2. Transformer': ['tap'],  # WT, OT could be added
    '3. Transformer LT': p_q_i,
    '4. Line': p_q_i,
    '5. Feeder': p_q_i,
    '6. Bank': p_q_i,
    '7. Breaker': ['Gas Pressure', 'Air Pressure', 'Spring Status']
}
for t in tuples:
    for reading in reading_dict[t[1]]:
        new_val = [reading]
        n = 6
        idx.append(list(t) + new_val)

idx1 = list(zip(*idx))
b = 5


b = 5
