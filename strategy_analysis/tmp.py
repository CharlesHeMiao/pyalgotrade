import numpy as np


def func():
    path = 'log/result.txt'
    with open(path, 'r') as f:
        lines = f.readlines()
    out_path = 'log/ret_sta.txt'
    sta = {i: [] for i in range(1, 6)}
    for line in lines:
        line = line.strip()
        group_index = int(line.split(',')[0].split()[1])
        value = float(line.split('$')[1])
        assert group_index in sta
        sta[group_index].append(value)

    with open(out_path, 'w') as of:
        for i in range(1, 6):
            vals = np.array(sta[i])
            mean = vals.mean()
            std = vals.std()
            of.write('group [%d]: mean [%.2f], std [%.2f], values %s\n' % (i, mean, std, sta[i]))


if __name__ == '__main__':
    func()
